#ifndef NEWSOURCEDIALOG_H_190731
#define NEWSOURCEDIALOG_H_190731
#include <QDialog>

class CodeEdit;
class QComboBox;
class QDialogButtonBox;
class QLineEdit;

class NewSourceDialog : public QDialog
{
  Q_OBJECT

  QDialogButtonBox *buttonBox;
  QLineEdit *nameEdit;
  CodeEdit *codeEdit;

public:
  NewSourceDialog(QWidget *parent = nullptr);

public slots:
  void clear();
protected slots:
  void checkValidity();
  void createSource();
};

#endif
