#ifndef CLONEDIALOG_190902
#define CLONEDIALOG_190902
#include <memory>
#include <string>
#include <QDialog>

class QLineEdit;
class QString;
namespace conf {
  class Value;
};

class CloneDialog : public QDialog
{
  Q_OBJECT

  // The extension:
  QString extension;
  // The name part (between "sources/" and extension):
  QString origName;

  // The value at the time the dialog was opened:
  std::shared_ptr<conf::Value const> value;

  QLineEdit *newKeyEdit;
  QPushButton *cloneButton;

public:
  CloneDialog(std::string const &, QWidget *parent = nullptr);

protected slots:
  void cloneSource();
  void validate();
};

#endif
