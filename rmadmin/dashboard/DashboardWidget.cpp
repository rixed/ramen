#include <QDebug>
#include <QMenu>
#include <QMenuBar>
#include <QPushButton>
#include <QVBoxLayout>
#include "AtomicWidget.h"
#include "conf.h"
#include "dashboard/DashboardCopyDialog.h"
#include "dashboard/DashboardSelector.h"
#include "dashboard/tools.h"
#include "Resources.h"

#include "dashboard/DashboardWidget.h"

static bool const verbose(true);

DashboardWidget::DashboardWidget(std::string const &widgetKey_, QWidget *parent)
  : AtomicForm(false, parent),
    widgetKey(widgetKey_)
{
  QMenuBar *menuBar = new QMenuBar;
  menuBar->addAction(tr("export"));
  QMenu *moveMenu = menuBar->addMenu(tr("move"));
  moveMenu->addSection(tr("Within this dashboard"));
  moveMenu->addAction(tr("Up")); // TODO: also icons of up arrow
  moveMenu->addAction(tr("Down"));
  moveMenu->addSection(tr("To another dashboard"));
  Resources const *r(Resources::get());
  moveMenu->addAction(r->copyPixmap, tr("Copy to…"),
                      this, &DashboardWidget::performCopy);
  moveMenu->addAction(tr("Move to…"),
                      this, &DashboardWidget::performMove);

  menuBar->setCornerWidget(deleteButton, Qt::TopRightCorner);
  menuBar->setCornerWidget(editButton, Qt::TopLeftCorner);

  menuFrame = new QWidget(this);
  layout = new QVBoxLayout;
  layout->setSpacing(0);
  layout->setContentsMargins(QMargins());
  layout->addWidget(menuBar, 0);
  menuFrame->setLayout(layout);

  // Prepare the copy destination window:
  /* FIXME: as this is an application-modal dialog we could have only one for
   * the whole app. */
  copyDialog = new DashboardCopyDialog(this);
}

void DashboardWidget::setCentralWidget(QWidget *w)
{
  layout->addWidget(w, 1);
  AtomicForm::setCentralWidget(menuFrame);
}

void DashboardWidget::doCopy(bool andDelete)
{
  if (QDialog::Accepted == copyDialog->exec(!andDelete)) {
    QString const dest_prefix_(copyDialog->dashSelector->currentData().toString());
    std::string const dest_prefix(dest_prefix_.toStdString());
    std::string const dest_key(
      dest_prefix +"/widgets/"+
      std::to_string(dashboardNextWidget(dest_prefix)));

    if (verbose)
      qDebug() << "DashboardWidget: Will copy to" << dest_prefix_;

    std::shared_ptr<conf::Value const> v = atomicWidget()->getValue();

    /* FIXME: wait for the new widget to be created. And to be sure that's
     * ours, lock and unlock the destination dashboard around these
     * operations, which implies to perform all this asynchronously. */
    askNew(dest_key, v);
    if (andDelete) askDel(widgetKey);
  }
}

void DashboardWidget::performCopy()
{
  doCopy(false);
}

void DashboardWidget::performMove()
{
  doCopy(true);
}
