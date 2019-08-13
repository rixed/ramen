#ifndef CONFTREEDIALOG_H_190731
#define CONFTREEDIALOG_H_190731
#include "SavedWindow.h"

class ConfTreeWidget;

class ConfTreeDialog : public SavedWindow
{
  Q_OBJECT

  ConfTreeWidget *confTreeWidget;

public:
  ConfTreeDialog(QWidget *parent = nullptr);
};

#endif
