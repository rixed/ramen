#include <cassert>
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
#include <QVector>
#include <QTimer>
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
          this, &ProcessesWidget::askAdjustColumnSize);
  connect(proxyModel, &ProcessesWidgetProxy::rowsInserted,
          this, &ProcessesWidget::adjustAllColumnSize);
  connect(proxyModel, &ProcessesWidgetProxy::rowsRemoved,
          this, &ProcessesWidget::adjustAllColumnSize);
  /* And when a row is expanded/collapsed: */
  connect(treeView, &QTreeView::expanded,
          this, &ProcessesWidget::adjustAllColumnSize);
  connect(treeView, &QTreeView::collapsed,
          this, &ProcessesWidget::adjustAllColumnSize);

  /* Don't wait for new keys to resize the columns: */
  for (int c = 0; c < GraphModel::NumColumns; c ++) {
    treeView->resizeColumnToContents(c);
  }

  /* Reset the filters when a function is added (or removed) (see comment
   * in ProcessesWidgetProxy.cpp as to why): */
  connect(graphModel, &GraphModel::rowsInserted,
          proxyModel, &ProcessesWidgetProxy::invalidate);
  connect(graphModel, &GraphModel::rowsRemoved,
          proxyModel, &ProcessesWidgetProxy::invalidate);

  /* Special signal when a worker changed, since that affects top-halfness
   * and working-ness: */
  connect(graphModel, &GraphModel::workerChanged,
          proxyModel, &ProcessesWidgetProxy::invalidate);

  /* Expand new entries. Unfortunately, does nothing to entries
   * that are no longer filtered. The proxy model stays completely
   * silent about those so there is little we can do. */
//  TODO: and now it crash. reactivate later
//  connect(graphModel, &GraphModel::rowsInserted,
//          this, &ProcessesWidget::expandRows);

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

  QVBoxLayout *mainLayout = new QVBoxLayout;
  mainLayout->setContentsMargins(0, 0, 0, 0);
  mainLayout->addWidget(searchFrame);
  mainLayout->addWidget(treeView);
  setLayout(mainLayout);

  adjustColumnTimer = new QTimer(this);
  adjustColumnTimer->setSingleShot(true);
  connect(adjustColumnTimer, &QTimer::timeout,
          this, &ProcessesWidget::adjustColumnSize);
}

/* Those ModelIndexes come from the GraphModel not the proxy, so no conversion
 * is necessary.
 * Actually does not adjust right now but use a timer to cluster changes as this
 * is a very expensive operation. */
void ProcessesWidget::askAdjustColumnSize(
  QModelIndex const &topLeft,
  QModelIndex const &bottomRight,
  QVector<int> const &roles)
{
  if (! roles.contains(Qt::DisplayRole)) return;

  assert(topLeft.column() >= 0 && bottomRight.column() >= 0);
  assert((size_t)bottomRight.column() < needResizing.size());
  for (size_t c = (size_t)topLeft.column(); c <= (size_t)bottomRight.column(); c ++) {
    needResizing.set(c);
  }

  static int const adjustColumnTimeout = 1000;
  adjustColumnTimer->start(adjustColumnTimeout);
}

void ProcessesWidget::adjustColumnSize()
{
  for (size_t c = 0; c < needResizing.size(); c ++) {
    if (needResizing.test(c)) {
      treeView->resizeColumnToContents(c);
      needResizing.reset(c);
    }
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

/* Return the QMainWindow or nullptr if, for some reason, the widget is in no
 * QMainWindow yet. */
static SavedWindow const *mySavedWindow(QWidget const *widget)
{
  if (! widget) return nullptr;

  SavedWindow const *win = static_cast<SavedWindow const *>(widget);
  if (win) return win;
  return mySavedWindow(widget);
}

void ProcessesWidget::wantEdit(std::shared_ptr<Program const> program)
{
  SavedWindow const *win = mySavedWindow(this);
  if (! win) {
    if (verbose)
      std::cerr << "Cannot find the main window!?" << std::endl;
    return;
  }

  if (! win->menu) {
    if (verbose)
      std::cerr << "Main Window has no menu!?" << std::endl;
    return;
  }

  win->menu->openRCEditor();
  win->menu->rcEditorDialog->preselect(program->name);
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

void ProcessesWidget::expandRows(QModelIndex const &parent, int first, int last)
{
  if (verbose)
    std::cout << "ProcessesWidget: Expanding children of "
              << (parent.isValid() ?
                    (static_cast<GraphItem *>(parent.internalPointer())->shared->
                                             name.toStdString()) :
                    "root")
              << " from rows " << first << " to " << last << std::endl;

  treeView->setExpanded(parent, true);

  for (int r = first; r <= last; r ++) {
    QModelIndex const index = parent.model()->index(r, 0, parent);
    // recursively:
    int const numChildren = index.model()->rowCount(index);
    expandRows(index, 0, numChildren - 1);
  }
}
