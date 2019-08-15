#ifndef NEWPROGRAMDIALOG_H_190731
#define NEWPROGRAMDIALOG_H_190731
#include <QDialog>
#include "KVPair.h"

class QPushButton;
class RCEntryEditor;

namespace conf {
  class Value;
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
  void appendEntry(std::shared_ptr<conf::Value>);

protected slots:
  void createProgram();
  void mayWriteRC(KVPair const &);
  // Called whenever the form is updated to maybe enable/disable the okButton:
  void validate();
};

#endif
