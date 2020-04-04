#ifndef NEWPROGRAMDIALOG_H_190731
#define NEWPROGRAMDIALOG_H_190731
#include <memory>
#include <QDialog>
#include "conf.h"

class QPushButton;
class RCEntryEditor;

struct KValue;
namespace conf {
  class Value;
};

class NewProgramDialog : public QDialog
{
  Q_OBJECT

  RCEntryEditor *editor;
  bool mustSave;

  QPushButton *okButton;

  void mayWriteRC(std::string const &, KValue const &);

public:
  NewProgramDialog(QString const &sourceName = "", QWidget *parent = nullptr);

private:
  void appendEntry(std::shared_ptr<conf::Value>);

protected slots:
  void createProgram();
  void onChange(QList<ConfChange> const &);
  // Called whenever the form is updated to maybe enable/disable the okButton:
  void validate();
};

#endif
