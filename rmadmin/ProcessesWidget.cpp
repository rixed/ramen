#include <cassert>
#include <QDebug>
#include <QDesktopWidget>
#include <QFrame>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QKeySequence>
#include <QLabel>
#include <QLineEdit>
#include <QModelIndex>
#include <QPalette>
#include <QPushButton>
#include <QtGlobal>
#include <QTimer>
#include <QTreeView>
#include <QVBoxLayout>
#include <QVector>
#include "ButtonDelegate.h"
#include "conf.h"
#include "dashboard/tools.h"
#include "FunctionItem.h"
#include "GraphModel.h"
#include "Menu.h"
#include "ProcessesWidgetProxy.h"
#include "ProgramItem.h"
#include "Resources.h"
#include "RCEditorDialog.h"
#include "SiteItem.h"
#include "TailTableDialog.h"
#include "UserIdentity.h"

#include "ProcessesWidget.h"

static bool const verbose(false);

/*
 * Now for the actual Processes list widget:
 */

ProcessesWidget::ProcessesWidget(GraphModel *graphModel, QWidget *parent) :
  QWidget(parent)
{
  /* Process list is large so the we overloaded the sizeHint and must
   * now explain how it's meant to be used: */
  setSizePolicy(QSizePolicy::Minimum, QSizePolicy::Minimum);

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
  treeView->header()->setStretchLastSection(true);

  /* The buttons just after the names.
   * If that's a program name then a button to edit the corresponding RC
   * entry. If that's a worker name then a button to open the tail view and
   * another one to open a chart. */
  treeView->setMouseTracking(true);  // for the buttons to follow the mouse
  ButtonDelegate *actionButton1 = new ButtonDelegate(3, this);
  treeView->setItemDelegateForColumn(GraphModel::ActionButton1, actionButton1);
  connect(actionButton1, &ButtonDelegate::clicked,
          this, &ProcessesWidget::activate1);

  ButtonDelegate *actionButton2 = new ButtonDelegate(3, this);
  treeView->setItemDelegateForColumn(GraphModel::ActionButton2, actionButton2);
  connect(actionButton2, &ButtonDelegate::clicked,
          this, &ProcessesWidget::activate2);

  /* Resize the columns to the _header_ content: */
  treeView->header()->setDefaultSectionSize(20); // For the 2 icons
  treeView->header()->setMinimumSectionSize(20);
  for (int c = 0; c < GraphModel::NumColumns; c ++) {
    if (c == GraphModel::Name) {
      treeView->header()->setSectionResizeMode(c, QHeaderView::ResizeToContents);
    } else if (c == GraphModel::ActionButton1 ||
               c == GraphModel::ActionButton2) {
      treeView->header()->setSectionResizeMode(c, QHeaderView::Fixed);
      // Redirect sorting attempt to first column:
      connect(treeView->header(), &QHeaderView::sortIndicatorChanged,
              this, [this](int c, Qt::SortOrder order) {
        if (c == GraphModel::ActionButton1 ||
            c == GraphModel::ActionButton2)
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
    if (c == GraphModel::ActionButton1 ||
        c == GraphModel::ActionButton2) continue;
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

  static int const adjustColumnTimeout = 1000; // ms
  adjustColumnTimer->start(adjustColumnTimeout);
}

void ProcessesWidget::adjustColumnSize()
{
  for (size_t c = 0; c < needResizing.size(); c ++) {
    if (needResizing.test(c)) {
      if (c != GraphModel::ActionButton1 &&
          c != GraphModel::ActionButton2)
        treeView->resizeColumnToContents(c);
      needResizing.reset(c);
    }
  }
}

void ProcessesWidget::adjustAllColumnSize()
{
  for (int c = 0; c < GraphModel::NumColumns; c ++) {
    if (c == GraphModel::ActionButton1 ||
        c == GraphModel::ActionButton2) continue;
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

  SavedWindow const *win = dynamic_cast<SavedWindow const *>(widget);
  if (win) return win;

  QWidget const *parent = widget->parentWidget();
  if (! parent) return nullptr;
  return mySavedWindow(parent);
}

void ProcessesWidget::wantEdit(std::shared_ptr<Program const> program)
{
  SavedWindow const *win = mySavedWindow(this);
  if (! win) {
    if (verbose)
      qCritical() << "Cannot find the main window!?";
    return;
  }

  if (! win->menu) {
    if (verbose)
      qCritical() << "Main Window has no menu!?";
    return;
  }

  win->menu->openRCEditor();
  win->menu->rcEditorDialog->preselect(program->name);
}

void ProcessesWidget::wantTable(std::shared_ptr<Function> function)
{
  std::shared_ptr<TailModel> tailModel = function->getOrCreateTail();
  if (tailModel) {
    TailTableDialog *dialog = new TailTableDialog(tailModel);
    dialog->show();
    dialog->raise();
  } else qWarning() << "Cannot create a TailModel";
}

void ProcessesWidget::wantChart(std::shared_ptr<Function> function)
{
  assert(my_socket);

  std::shared_ptr<conf::DashWidgetChart> chart =
    std::make_shared<conf::DashWidgetChart>(
      function->siteName.toStdString(),
      function->programName.toStdString(),
      function->name.toStdString());

  /* The only way to display a chart is from a dashboard (where its definition
   * is stored). So this button merely adds a new chart to the scratchpad
   * dashboard and opens it: */
  std::string const dash_key("clients/" + *my_socket + "/scratchpad");
  int const num = dashboardNextWidget(dash_key);
  std::string widget_key(dash_key + "/widgets/" + std::to_string(num));
  /* No need to lock in theory, as the scratchpad is per socket, but
   * lock ownership is how we know to activate the editor: */
  askNew(widget_key, std::dynamic_pointer_cast<conf::Value const>(chart),
         DEFAULT_LOCK_TIMEOUT);
  /* And opens it */
  Menu::openDashboard(QString("scratchpad"), dash_key);
}

void ProcessesWidget::activate(QModelIndex const &proxyIndex, int button)
{
  // Retrieve the function or program:
  QModelIndex const index = proxyModel->mapToSource(proxyIndex);
  GraphItem *parentPtr =
    static_cast<GraphItem *>(index.internalPointer());

  ProgramItem const *programItem =
    dynamic_cast<ProgramItem const *>(parentPtr);

  if (programItem && button == 2) {
    wantEdit(std::static_pointer_cast<Program>(programItem->shared));
    return;
  }

  FunctionItem *functionItem =
    dynamic_cast<FunctionItem *>(parentPtr);

  if (functionItem) {
    if (button == 1)
      wantTable(std::static_pointer_cast<Function>(functionItem->shared));
    else
      wantChart(std::static_pointer_cast<Function>(functionItem->shared));
    return;
  }

  qCritical() << "Activate an unknown object!?";
}

void ProcessesWidget::activate1(QModelIndex const &proxyIndex)
{
  activate(proxyIndex, 1);
}

void ProcessesWidget::activate2(QModelIndex const &proxyIndex)
{
  activate(proxyIndex, 2);
}

void ProcessesWidget::expandRows(QModelIndex const &parent, int first, int last)
{
  if (verbose)
    qDebug() << "ProcessesWidget: Expanding children of"
              << (parent.isValid() ?
                    (static_cast<GraphItem *>(
                      parent.internalPointer())->shared->name) : "root")
              << "from rows" << first << "to" << last;

  treeView->setExpanded(parent, true);

  for (int r = first; r <= last; r ++) {
    QModelIndex const index = parent.model()->index(r, 0, parent);
    // recursively:
    int const numChildren = index.model()->rowCount(index);
    expandRows(index, 0, numChildren - 1);
  }
}
