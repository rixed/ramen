#ifndef CONFTREEDIALOG_H_190731
#define CONFTREEDIALOG_H_190731
#include <QMainWindow>

class ConfTreeWidget;

class ConfTreeDialog : public QMainWindow
{
  Q_OBJECT

  ConfTreeWidget *confTreeWidget;

public:
  ConfTreeDialog(QWidget *parent = nullptr);
};

#endif
