#include "dashboard/Dashboard.h"
#include "dashboard/DashboardWindow.h"

DashboardWindow::DashboardWindow(
  QString const &name,
  std::string const &key_prefix,
  QWidget *parent)
  : QMainWindow(parent)
{
  setWindowTitle(name);

  dashboard = new Dashboard(key_prefix, this);
  setCentralWidget(dashboard);
}
