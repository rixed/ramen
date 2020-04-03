#include <QDebug>
#include <QMenu>
#include <QMenuBar>
#include <QPushButton>
#include <QVBoxLayout>
#include "AtomicWidget.h"
#include "conf.h"
#include "dashboard/DashboardCopyDialog.h"
#include "dashboard/DashboardSelector.h"
#include "dashboard/DashboardWidget.h"
#include "dashboard/tools.h"
#include "Resources.h"

#include "dashboard/DashboardWidgetForm.h"

static bool const verbose(false);

DashboardWidgetForm::DashboardWidgetForm(
  std::string const &widgetKey_,
  Dashboard *dashboard_,
  QWidget *parent)
  : AtomicForm(false, parent),
    widgetKey(widgetKey_),
    dashboard(dashboard_)
{
  widget = new DashboardWidget(dashboard, this, this);
  widget->setKey(widgetKey);
  addWidget(widget, true);

  QMenuBar *menuBar = new QMenuBar;
  menuBar->addAction(tr("export"));
  QMenu *moveMenu = menuBar->addMenu(tr("move"));
  moveMenu->addSection(tr("Within this dashboard"));

  Resources const *r(Resources::get());

  upAction = moveMenu->addAction(r->upPixmap, tr("Up"),
                                 this, &DashboardWidgetForm::moveUp);
  downAction = moveMenu->addAction(r->downPixmap, tr("Down"),
                                   this, &DashboardWidgetForm::moveDown);
  moveMenu->addSection(tr("To another dashboard"));
  moveMenu->addAction(r->copyPixmap, tr("Copy to…"),
                      this, &DashboardWidgetForm::performCopy);
  moveMenu->addAction(tr("Move to…"),
                      this, &DashboardWidgetForm::performMove);

  menuBar->setCornerWidget(deleteButton, Qt::TopRightCorner);
  menuBar->setCornerWidget(editButton, Qt::TopLeftCorner);

  menuFrame = new QWidget(this);
  layout = new QVBoxLayout;
  layout->setSpacing(0);
  layout->setContentsMargins(QMargins());
  layout->addWidget(menuBar, 0);
  layout->addWidget(widget, 1);
  menuFrame->setLayout(layout);
  setCentralWidget(menuFrame);

  // Prepare the copy destination window:
  /* FIXME: as this is an application-modal dialog we could have only one for
   * the whole app. */
  copyDialog = new DashboardCopyDialog(this);
}

DashboardWidgetForm::~DashboardWidgetForm()
{
  delete widget;
}

void DashboardWidgetForm::enableArrowsForPosition(size_t idx, size_t count)
{
  upAction->setEnabled(idx > 0);
  downAction->setEnabled(idx < count - 1);
}

void DashboardWidgetForm::doCopy(bool andDelete)
{
  if (QDialog::Accepted == copyDialog->exec(!andDelete)) {
    QString const dest_prefix_(copyDialog->dashSelector->currentData().toString());
    std::string const dest_prefix(dest_prefix_.toStdString());
    std::string const dest_key(
      dest_prefix +"/widgets/"+
      std::to_string(dashboardNextWidget(dest_prefix)));

    if (verbose)
      qDebug() << "DashboardWidgetForm: Will copy to" << dest_prefix_;

    std::shared_ptr<conf::Value const> v = widget->getValue();

    /* FIXME: wait for the new widget to be created. And to be sure that's
     * ours, lock and unlock the destination dashboard around these
     * operations, which implies to perform all this asynchronously. */
    askNew(dest_key, v);
    if (andDelete) askDel(widgetKey);
  }
}

void DashboardWidgetForm::performCopy()
{
  doCopy(false);
}

void DashboardWidgetForm::performMove()
{
  doCopy(true);
}

void DashboardWidgetForm::moveUp()
{
  /* Locate which other widget to switch position with: */
  std::optional<int> const myIdx(widgetIndexOfKey(widgetKey));
  if (! myIdx) {
    qCritical("Cannot find out widget index from %s?!", widgetKey.c_str());
    return;
  }

  // TODO: lock the whole dashboard first:
  std::string destKey;
  KValue destVal;
  std::string const prefix(dashboardPrefixOfKey(widgetKey));
  iterDashboardWidgets(prefix,
    [myIdx, &destKey, &destVal](std::string const &key, KValue const &val, int idx) {
    if (idx < *myIdx) {
      destKey = key;
      destVal = val;
    }
  });

  switchPosition(destKey, destVal);
}

void DashboardWidgetForm::moveDown()
{
  /* Locate which other widget to switch position with: */
  std::optional<int> const myIdx(widgetIndexOfKey(widgetKey));
  if (! myIdx) {
    qCritical("Cannot find out widget index from %s?!", widgetKey.c_str());
    return;
  }

  // TODO: lock the whole dashboard first:
  std::string destKey;
  KValue destVal;
  std::string const prefix(dashboardPrefixOfKey(widgetKey));
  iterDashboardWidgets(prefix,
    [myIdx, &destKey, &destVal](std::string const &key, KValue const &val, int idx) {
    if (idx > *myIdx && destKey.empty()) {
      destKey = key;
      destVal = val;
    }
  });

  switchPosition(destKey, destVal);
}

void DashboardWidgetForm::switchPosition(
  std::string const &destKey, KValue const &destVal)
{
  if (destKey.empty()) {
    qCritical("No widget to switch position with %s?!", widgetKey.c_str());
    return;
  }

  askSet(destKey, widget->getValue());
  askSet(widgetKey, destVal.val);
}
