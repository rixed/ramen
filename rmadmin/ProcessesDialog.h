#ifndef PROCESSESDIALOG_H_190806
#define PROCESSESDIALOG_H_190806
#include <QDialog>

class ProcessesWidget;
class GraphModel;

class ProcessesDialog : public QDialog
{
  Q_OBJECT

  ProcessesWidget *processesWidget;

public:
  ProcessesDialog(GraphModel *, QWidget *parent = nullptr);

  void keyPressEvent(QKeyEvent *);
};

#endif
