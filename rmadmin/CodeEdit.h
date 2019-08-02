#ifndef CODEEDIT_H_190516
#define CODEEDIT_H_190516
#include <QWidget>
#include "confKey.h"
#include "KValue.h"

class ProgramItem;
class KTextEdit;
class AtomicForm;

class CodeEdit : public QWidget
{
  Q_OBJECT

  QString const sourceName;
  conf::Key const keyText;

  KTextEdit *textEdit;
  AtomicForm *editorForm;

public:
  CodeEdit(conf::Key const &, QWidget *parent = nullptr);
};

#endif
