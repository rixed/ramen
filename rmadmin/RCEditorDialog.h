#ifndef RCEDITORDIALOG_H_190809
#define RCEDITORDIALOG_H_190809
#include <QMainWindow>

class TargetConfigEditor;
class QMessageBox;

class RCEditorDialog : public QMainWindow
{
  Q_OBJECT

  TargetConfigEditor *targetConfigEditor;

  QMessageBox *confirmDeleteDialog;
public:

  RCEditorDialog(QWidget *parent = nullptr);

protected slots:
  void wantDeleteEntry();
};

#endif
