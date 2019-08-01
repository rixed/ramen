#ifndef NEWPROGRAMDIALOG_H_190731
#define NEWPROGRAMDIALOG_H_190731
#include <QDialog>
#include <QString>

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

public:
  NewProgramDialog(QWidget *parent = nullptr);

private:
  void appendEntry();

protected slots:
  void createProgram();
  void mayWriteRC(conf::Key const &, QString const &uid, double expiry);
};

#endif
