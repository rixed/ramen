#include <QDebug>
#include "dashboard/Dashboard.h"

#include "dashboard/DashboardWindow.h"

static bool const verbose { false };

DashboardWindow::DashboardWindow(
  QString const &name,
  std::string const &key_prefix,
  QWidget *parent)
  : SavedWindow(
      QString("dashboardWindow/") + name,
      QString("Dashboard: ") + name,
      true,
      parent,
      true)
{
  dashboard = new Dashboard(key_prefix, this);
  setCentralWidget(dashboard);

  resize(700, 700);

  if (verbose)
    qDebug() << "New dashboard window created";
}
