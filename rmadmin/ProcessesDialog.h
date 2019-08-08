#ifndef PROCESSESDIALOG_H_190806
#define PROCESSESDIALOG_H_190806
#include <QMainWindow>

class ProcessesWidget;
class GraphModel;

class ProcessesDialog : public QMainWindow
{
  Q_OBJECT

  ProcessesWidget *processesWidget;

public:
  ProcessesDialog(GraphModel *, QWidget *parent = nullptr);

  void keyPressEvent(QKeyEvent *);
};

#endif
