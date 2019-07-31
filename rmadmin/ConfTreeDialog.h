#ifndef CONFTREEDIALOG_H_190731
#define CONFTREEDIALOG_H_190731
#include <QDialog>

class ConfTreeWidget;

class ConfTreeDialog : public QDialog
{
  Q_OBJECT

  ConfTreeWidget *confTreeWidget;

public:
  ConfTreeDialog(QWidget *parent = nullptr);
};

#endif
