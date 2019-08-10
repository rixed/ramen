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
#include <QSortFilterProxyModel>
#include <QHBoxLayout>
#include <QVBoxLayout>
#include <QMenuBar>
#include <QMenu>
#include <QVector>
#include "Resources.h"
#include "GraphModel.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "SiteItem.h"
#include "ButtonDelegate.h"
#include "ProcessesWidget.h"

/*
 * Start by creating a filter proxy that would filter parents or
 * children; as opposed to the default one that filter children only
 * if their parent passes the filter.
 */

class MyProxy : public QSortFilterProxyModel
{
  bool includeTopHalves, includeStopped;

public:
  MyProxy(QObject * = nullptr);

protected:
  bool filterAcceptsRow(int, QModelIndex const &) const override;

public slots:
  void viewTopHalves(bool checked);
  void viewStopped(bool checked);
};

MyProxy::MyProxy(QObject *parent) : QSortFilterProxyModel(parent)
{}

bool MyProxy::filterAcceptsRow(int sourceRow, QModelIndex const &sourceParent) const
{
  if (! sourceParent.isValid()) return true;

  /* For now keep it simple: Accept all sites and programs, filter only
   * function names. */
  GraphItem const *parentPtr =
    static_cast<GraphItem const *>(sourceParent.internalPointer());

  SiteItem const *parentSite =
    dynamic_cast<SiteItem const *>(parentPtr);
  if (parentSite) {
    /* If that program is running top-halves then also filter it: */
    assert((size_t)sourceRow < parentSite->programs.size());
    ProgramItem const *program = parentSite->programs[sourceRow];
    if (! includeTopHalves && program->isTopHalf()) return false;
    if (! includeStopped && ! program->isWorking()) return false;
    return true;
  }

  ProgramItem const *parentProgram =
    dynamic_cast<ProgramItem const *>(parentPtr);
  if (! parentProgram) {
    std::cout << "Filtering the rows of a function?!" << std::endl;
    return false;
  }

  /* When the parent is a program, build the FQ name of the function
   * and match that: */
  assert((size_t)sourceRow < parentProgram->functions.size());
  FunctionItem const *function = parentProgram->functions[sourceRow];

  // Filter out the top-halves, optionally:
  if (! includeTopHalves && function->isTopHalf()) return false;
  if (! includeStopped && ! function->isWorking()) return false;

  SiteItem const *site =
    static_cast<SiteItem const *>(parentProgram->treeParent);

  QString const fq(site->name + ":" + parentProgram->name + "/" +
                   function->name);
  std::cout << "FILTER " << fq.toStdString() << std::endl;
  return fq.contains(filterRegExp());
}

void MyProxy::viewTopHalves(bool checked)
{
  includeTopHalves = checked;
  invalidateFilter();
}

void MyProxy::viewStopped(bool checked)
{
  includeStopped = checked;
  invalidateFilter();
}

/*
 * Now for the actual Processes list widget:
 */

ProcessesWidget::ProcessesWidget(GraphModel *graphModel, QWidget *parent) :
  QWidget(parent)
{
  treeView = new QTreeView;
  proxyModel = new MyProxy(this);
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

  /* The buttons just after the names: */
  treeView->setMouseTracking(true);  // for the buttons to follow the mouse
  ButtonDelegate *editButton = new ButtonDelegate(3, this);
  treeView->setItemDelegateForColumn(GraphModel::EditButton, editButton);
  connect(editButton, &ButtonDelegate::clicked,
          this, &ProcessesWidget::wantEdit);
  ButtonDelegate *tableButton = new ButtonDelegate(3, this);
  treeView->setItemDelegateForColumn(GraphModel::TableButton, tableButton);

  /* Resize the columns to the _header_ content: */
  for (int c = 0; c < GraphModel::NumColumns; c ++) {
    if (c == 0) {
      treeView->header()->setSectionResizeMode(c, QHeaderView::Stretch);
    } else if (c < GraphModel::EndButtons) {
      treeView->header()->setSectionResizeMode(c, QHeaderView::Fixed);
      treeView->header()->setDefaultSectionSize(15);
      // Redirect sorting attempt to first column:
      connect(treeView->header(), &QHeaderView::sortIndicatorChanged,
              this, [this](int c, Qt::SortOrder order) {
        if (c >= GraphModel::FirstButton &&
            c < GraphModel::EndButtons) {
          treeView->header()->setSortIndicator(0, order);
        }
      });
    } else {
      treeView->header()->setSectionResizeMode(c, QHeaderView::ResizeToContents);
    }
  }

  /* Now also resize the column to the data content: */
  connect(graphModel, &GraphModel::dataChanged,
          this, &ProcessesWidget::adjustColumnSize);

  /* Don't wait for new keys to resize the columns: */
  for (int c = 0; c < GraphModel::NumColumns; c ++) {
    treeView->resizeColumnToContents(c);
  }

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
  //QCoreApplication::setAttribute(Qt::AA_DontUseNativeMenuBar, true);
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
      proxyModel, &MyProxy::viewTopHalves);
  viewTopHalves->setCheckable(true);
  viewTopHalves->setChecked(false);
  proxyModel->viewTopHalves(false);

  QAction *viewStopped =
    viewMenu->addAction(
      QCoreApplication::translate("QMenuBar", "Stopped"),
      proxyModel, &MyProxy::viewStopped);
  viewStopped->setCheckable(true);
  viewStopped->setChecked(false);
  proxyModel->viewStopped(false);

  viewMenu->addSeparator();
  for (unsigned c = 0; c < GraphModel::NumColumns; c ++) {
    if (c < GraphModel::EndButtons) continue; // Name and buttons are mandatory

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
  //QCoreApplication::setAttribute(Qt::AA_DontUseNativeMenuBar, false);

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

void ProcessesWidget::wantEdit(QModelIndex const &proxyIndex)
{
  // Retrieve the program:
  QModelIndex const index = proxyModel->mapToSource(proxyIndex);
  GraphItem const *parentPtr =
    static_cast<GraphItem const *>(index.internalPointer());

/*  SiteItem const *parentSite =
    dynamic_cast<SiteItem const *>(parentPtr);
  if (! parentSite) {
    std::cout << "Editing signalled for a non-ProgramItem!?" << std::endl;
    return;
  }

  if (index.row() >= (int)parentSite->programs.size()) {
    std::cout << "wantEdit an non-existent program!?" << std::endl;
    return;
  }
  ProgramItem const *program = parentSite->programs[index.row()];
*/

  std::cout << "WANTEDIT row=" << index.row() << std::endl;

  ProgramItem const *program =
    dynamic_cast<ProgramItem const *>(parentPtr);

  if (! program) {
    std::cout << "Editing signalled for a non-ProgramItem!?" << std::endl;
    return;
  }

  std::cout << "TODO: open the RC editor preselcting the entry for "
            << program->name.toStdString() << std::endl;
}
