#include "RamenValueEditor.h"

RamenValueEditor *RamenValueEditor::ofType(enum RamenTypeStructure type, RamenValue const *val, QWidget *parent)
{
  RamenValueEditor *editor = new RamenValueEditor(type, parent);
  editor->setText(val->toQString());
  return editor;
}

RamenValue *RamenValueEditor::getValue() const
{
  return RamenValue::ofQString(type, text());
}
