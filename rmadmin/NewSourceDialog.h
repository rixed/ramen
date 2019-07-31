#ifndef NEWSOURCEDIALOG_H_190731
#define NEWSOURCEDIALOG_H_190731
#include <QDialog>

class QLineEdit;
class QComboBox;

class NewSourceDialog : public QDialog
{
  Q_OBJECT

  QLineEdit *nameEdit;
  QComboBox *typeEdit;

public:
  NewSourceDialog(QWidget *parent = nullptr);

protected slots:
  void createSource();
};

#endif
