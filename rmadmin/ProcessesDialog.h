#ifndef PROCESSESDIALOG_H_190806
#define PROCESSESDIALOG_H_190806
#include "SavedWindow.h"

class ProcessesWidget;
class GraphModel;

class ProcessesDialog : public SavedWindow
{
  Q_OBJECT

  ProcessesWidget *processesWidget;

public:
  ProcessesDialog(GraphModel *, QWidget *parent = nullptr);

protected:
  void keyPressEvent(QKeyEvent *);
};

#endif
