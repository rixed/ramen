#ifndef CODEEDIT_H_190516
#define CODEEDIT_H_190516
#include <QWidget>
#include "confKey.h"
#include "KValue.h"

class ProgramItem;
class KTextEdit;
class AtomicForm;
class QLabel;

class CodeEdit : public QWidget
{
  Q_OBJECT

  QString const sourceName;
  conf::Key const keyText;

  KTextEdit *textEdit;
  AtomicForm *editorForm;
  QLabel *compilationError;

public:
  CodeEdit(conf::Key const &, QWidget *parent = nullptr);

protected slots:
  void setError(conf::Key const &, std::shared_ptr<conf::Value const>, QString const &, double);
};

#endif
