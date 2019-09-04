#ifndef CODEEDITFORM_H_190516
#define CODEEDITFORM_H_190516
#include <memory>
#include <string>
#include <QWidget>
#include "CodeEdit.h"

class AtomicForm;
class CodeEdit;

class CodeEditForm : public QWidget
{
  Q_OBJECT

public:
  CodeEdit *codeEdit;
  AtomicForm *editorForm;
  CodeEditForm(QWidget *parent = nullptr);

protected slots:
  void wantClone();
};

#endif
