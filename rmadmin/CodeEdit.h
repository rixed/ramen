#ifndef CODEEDIT_H_190516
#define CODEEDIT_H_190516
#include <string>
#include <QWidget>
#include "KVPair.h"

class ProgramItem;
class KTextEdit;
class AtomicForm;
class QLabel;

class CodeEdit : public QWidget
{
  Q_OBJECT

public:
  QString const sourceName;
  std::string textKey;
  std::string infoKey;

  KTextEdit *textEdit;
  AtomicForm *editorForm;
  QLabel *compilationError;

  CodeEdit(QWidget *parent = nullptr);

protected:
  void resetError(KValue const *);

public slots:
  void setKey(std::string const &);

protected slots:
  void setError(KVPair const &);
};

#endif
