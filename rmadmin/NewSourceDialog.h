#ifndef NEWSOURCEDIALOG_H_190731
#define NEWSOURCEDIALOG_H_190731
#include <QDialog>

class QLineEdit;
class QComboBox;
class CodeEdit;

class NewSourceDialog : public QDialog
{
  Q_OBJECT

  QLineEdit *nameEdit;
  CodeEdit *codeEdit;

public:
  NewSourceDialog(QWidget *parent = nullptr);
  void clear();

protected slots:
  void createSource();
};

#endif
