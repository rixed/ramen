#ifndef DASHBOARDCOPYDIALOG_H_200322
#define DASHBOARDCOPYDIALOG_H_200322
#include <QDialog>

class DashboardSelector;
class QRadioButton;

class DashboardCopyDialog : public QDialog
{
  Q_OBJECT

  QRadioButton *copyButton;
  QRadioButton *moveButton;

public:
  DashboardSelector *dashSelector;

  DashboardCopyDialog(QWidget *parent = nullptr);

  int copy(bool defaultToCopy = true);
};

#endif
