#ifndef NEWPROGRAMDIALOG_H_190731
#define NEWPROGRAMDIALOG_H_190731
#include <QDialog>

class QPushButton;
class RCEntryEditor;
class KValue;
namespace conf {
  class Key;
};

class NewProgramDialog : public QDialog
{
  Q_OBJECT

  RCEntryEditor *editor;
  bool mustSave;

  QPushButton *okButton;

public:
  NewProgramDialog(QString const &sourceName = "", QWidget *parent = nullptr);

private:
  void appendEntry();

protected slots:
  void createProgram();
  void mayWriteRC(conf::Key const &, QString const &uid, double expiry);
  // Called whenever the form is updated to maybe enable/disable the okButton:
  void validate();
};

#endif
