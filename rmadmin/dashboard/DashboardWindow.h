#ifndef DASHBOARDWINDOW_H_200304
#define DASHBOARDWINDOW_H_200304
#include <string>
#include <QMainWindow>
#include <QString>

class Dashboard;

class DashboardWindow : public QMainWindow
{
  Q_OBJECT

  Dashboard *dashboard;

public:
  DashboardWindow(
    QString const &name,
    std::string const &key_prefix,
    QWidget *parent = nullptr);

};

#endif
