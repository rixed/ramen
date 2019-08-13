#include <iostream>
#include <cassert>
#include <QTreeView>
#include <QKeySequence>
#include <QFrame>
#include <QLineEdit>
#include <QLabel>
#include <QPushButton>
#include <QPalette>
#include <QHeaderView>
#include <QModelIndex>
#include <QHBoxLayout>
#include <QVBoxLayout>
#include <QMenuBar>
#include <QMenu>
#include <QVector>
#include "Resources.h"
#include "Menu.h"
#include "GraphModel.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "SiteItem.h"
#include "ButtonDelegate.h"
#include "RCEditorDialog.h"
#include "TailTableDialog.h"
#include "ProcessesWidgetProxy.h"
#include "ProcessesWidget.h"

static bool const verbose = true;

/*
 * Now for the actual Processes list widget:
 */

ProcessesWidget::ProcessesWidget(GraphModel *graphModel, QWidget *parent) :
  QWidget(parent)
{
  treeView = new QTreeView;
  proxyModel = new ProcessesWidgetProxy(this);
  proxyModel->setSourceModel(graphModel);
  proxyModel->setSortRole(GraphModel::SortRole);
  // Also filter on the name as displayed:
  proxyModel->setFilterKeyColumn(0);
  treeView->setModel(proxyModel);
  treeView->setUniformRowHeights(true);
  treeView->setAlternatingRowColors(true);
  treeView->setSortingEnabled(true);
  treeView->setSelectionBehavior(QAbstractItemView::SelectRows);
  treeView->expandAll();
  treeView->header()->setStretchLastSection(false);

  /* The buttons just after the names.
   * If that's a program name then a button to edit the corresponding RC
   * entry. If that's a worker name then a button to open the tail view. */
  treeView->setMouseTracking(true);  // for the buttons to follow the mouse
  ButtonDelegate *actionButton = new ButtonDelegate(3, this);
  treeView->setItemDelegateForColumn(GraphModel::ActionButton, actionButton);
  connect(actionButton, &ButtonDelegate::clicked,
          this, &ProcessesWidget::activate);

  /* Resize the columns to the _header_ content: */
  for (int c = 0; c < GraphModel::NumColumns; c ++) {
    if (c == 0) {
      treeView->header()->setSectionResizeMode(c, QHeaderView::Stretch);
    } else if (c == GraphModel::ActionButton) {
      treeView->header()->setSectionResizeMode(c, QHeaderView::Fixed);
      treeView->header()->setDefaultSectionSize(15);
      // Redirect sorting attempt to first column:
      connect(treeView->header(), &QHeaderView::sortIndicatorChanged,
              this, [this](int c, Qt::SortOrder order) {
        if (c == GraphModel::ActionButton)
          treeView->header()->setSortIndicator(0, order);
      });
    } else {
      treeView->header()->setSectionResizeMode(c, QHeaderView::ResizeToContents);
    }
  }

  /* Now also resize the column to the data content: */
  connect(proxyModel, &ProcessesWidgetProxy::dataChanged,
          this, &ProcessesWidget::adjustColumnSize);
  connect(proxyModel, &ProcessesWidgetProxy::rowsInserted,
          this, &ProcessesWidget::adjustAllColumnSize);
  connect(proxyModel, &ProcessesWidgetProxy::rowsRemoved,
          this, &ProcessesWidget::adjustAllColumnSize);

  /* Don't wait for new keys to resize the columns: */
  for (int c = 0; c < GraphModel::NumColumns; c ++) {
    treeView->resizeColumnToContents(c);
  }

  /* Reset the filters when a function is added (or removed) */
/*  connect(graphModel, &GraphModel::rowsInserted,
          proxyModel, &ProcessesWidgetProxy::invalidate);
  connect(graphModel, &GraphModel::rowsRemoved,
          proxyModel, &ProcessesWidgetProxy::invalidate);*/

  /* Expand new entries. Unfortunately, does nothing to entries
   * that are no longer filtered. The proxy model stays completely
   * silent about those so there is little we can do. */
  connect(graphModel, &GraphModel::rowsInserted,
          this, &ProcessesWidget::expandRows);

  /* Special signal when a worker changed, since that affects top-halfness
   * and working-ness: */
  connect(graphModel, &GraphModel::workerChanged,
          proxyModel, &ProcessesWidgetProxy::invalidate);

  /*
   * Searchbox, hidden when unused
   */
  searchFrame = new QWidget(this);
  searchFrame->setContentsMargins(0, 0, 0, 0);
  QHBoxLayout *searchLayout = new QHBoxLayout;
  searchLayout->setContentsMargins(0, 0, 0, 0);
  QLabel *image = new QLabel;
  image->setPixmap(resources->get()->searchPixmap);
  image->setContentsMargins(0, 0, 0, 0);
  searchLayout->addStretch();
  searchLayout->addWidget(image);
  searchBox = new QLineEdit;
  searchBox->setPlaceholderText(tr("Search"));
  searchBox->setFixedWidth(200);
  searchBox->setContentsMargins(0, 0, 0, 0);
  searchLayout->addWidget(searchBox);
  QIcon closeIcon(resources->get()->closePixmap);
  QPushButton *closeButton = new QPushButton;
  closeButton->setIcon(closeIcon);
  closeButton->setStyleSheet("padding: 1px;");
  searchLayout->addWidget(closeButton);
  searchFrame->setLayout(searchLayout);
  searchFrame->adjustSize();
  searchFrame->hide();
  connect(searchBox, &QLineEdit::textChanged,
          this, &ProcessesWidget::changeSearch);
  connect(closeButton, &QPushButton::clicked,
          this, &ProcessesWidget::closeSearch);

  /* The menu bar */
  QMenuBar *menuBar = new QMenuBar(this);
  QMenu *viewMenu = menuBar->addMenu(
    QCoreApplication::translate("QMenuBar", "&View"));

  viewMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Search…"),
    this, &ProcessesWidget::openSearch,
    QKeySequence::Find);

  QAction *viewTopHalves =
    viewMenu->addAction(
      QCoreApplication::translate("QMenuBar", "Top-Halves"),
      proxyModel, &ProcessesWidgetProxy::viewTopHalves);
  viewTopHalves->setCheckable(true);
  viewTopHalves->setChecked(false);
  proxyModel->viewTopHalves(false);

  QAction *viewStopped =
    viewMenu->addAction(
      QCoreApplication::translate("QMenuBar", "Stopped"),
      proxyModel, &ProcessesWidgetProxy::viewStopped);
  viewStopped->setCheckable(true);
  viewStopped->setChecked(false);
  proxyModel->viewStopped(false);

  viewMenu->addSeparator();
  for (unsigned c = 0; c < GraphModel::NumColumns; c ++) {
    if (c == GraphModel::ActionButton) continue; // Name and buttons are mandatory

    QString const name = GraphModel::columnName((GraphModel::Columns)c);
    // Column names have already been translated
    QAction *toggle =
      viewMenu->addAction(name, [this, c](bool checked) {
        treeView->setColumnHidden(c, !checked);
      });
    toggle->setCheckable(true);
    if (GraphModel::columnIsImportant((GraphModel::Columns)c)) {
      toggle->setChecked(true);
    } else {
      treeView->setColumnHidden(c, true);
    }
  }
  /* Although it seems to be the last entry, Qt will actually add some more
   * on MacOS ("enter full screen"): */
  viewMenu->addSeparator();

  QVBoxLayout *mainLayout = new QVBoxLayout;
  mainLayout->setContentsMargins(0, 0, 0, 0);
  mainLayout->addWidget(searchFrame);
  mainLayout->addWidget(treeView);
  mainLayout->setMenuBar(menuBar);
  setLayout(mainLayout);
}

/* Those ModelIndexes come from the GraphModel not the proxy, so no conversion
 * is necessary. */
void ProcessesWidget::adjustColumnSize(
  QModelIndex const &topLeft,
  QModelIndex const &bottomRight,
  QVector<int> const &roles)
{
  if (! roles.contains(Qt::DisplayRole)) return;

  for (int c = topLeft.column(); c <= bottomRight.column(); c ++) {
    treeView->resizeColumnToContents(c);
  }
}

void ProcessesWidget::adjustAllColumnSize()
{
  for (int c = 0; c < GraphModel::NumColumns; c ++) {
    treeView->resizeColumnToContents(c);
  }
}

void ProcessesWidget::openSearch()
{
  searchFrame->show();
  searchBox->setFocus();
}

void ProcessesWidget::changeSearch(QString const &text)
{
  proxyModel->setFilterFixedString(text);
  if (text.length() > 0) treeView->expandAll();
}

void ProcessesWidget::closeSearch()
{
  searchFrame->hide();
  searchBox->clear();
}

void ProcessesWidget::wantEdit(std::shared_ptr<Program const> program)
{
  globalMenu->openRCEditor();
  globalMenu->rcEditorDialog->preselect(program->name);
}

void ProcessesWidget::wantTable(std::shared_ptr<Function> function)
{
  std::shared_ptr<TailModel> tailModel = function->getTail();
  if (tailModel) {
    TailTableDialog *dialog = new TailTableDialog(tailModel);
    dialog->show();
    dialog->raise();
  }
}

void ProcessesWidget::activate(QModelIndex const &proxyIndex)
{
  // Retrieve the function or program:
  QModelIndex const index = proxyModel->mapToSource(proxyIndex);
  GraphItem *parentPtr =
    static_cast<GraphItem *>(index.internalPointer());

  ProgramItem const *programItem =
    dynamic_cast<ProgramItem const *>(parentPtr);

  if (programItem) {
    wantEdit(std::static_pointer_cast<Program>(programItem->shared));
    return;
  }

  FunctionItem *functionItem =
    dynamic_cast<FunctionItem *>(parentPtr);

  if (functionItem) {
    wantTable(std::static_pointer_cast<Function>(functionItem->shared));
    return;
  }

  std::cerr << "Activate an unknown object!?" << std::endl;
}

void ProcessesWidget::expandRows(const QModelIndex &parent, int first, int last)
{
  if (verbose)
    std::cout << "Expanding children of "
              << (static_cast<GraphItem *>(parent.internalPointer())->shared->
                                           name.toStdString())
              << " from rows " << first << " to " << last << std::endl;

  for (int r = first; r <= last; r ++) {
    QModelIndex const index = parent.model()->index(r, 0, parent);
    treeView->setExpanded(index, true);
    // recursively:
    int const numChildren = index.model()->rowCount(index);
    expandRows(index, 0, numChildren);;
  }
}
