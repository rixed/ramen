#ifndef RCEDITORDIALOG_H_190809
#define RCEDITORDIALOG_H_190809
#include <QMainWindow>

class TargetConfigEditor;
class QMessageBox;
class QString;

class RCEditorDialog : public QMainWindow
{
  Q_OBJECT

  TargetConfigEditor *targetConfigEditor;

  QMessageBox *confirmDeleteDialog;
public:
  RCEditorDialog(QWidget *parent = nullptr);

  void preselect(QString const &programName);

protected slots:
  void wantDeleteEntry();
};

#endif
