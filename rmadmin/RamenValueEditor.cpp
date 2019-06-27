#include "RamenValueEditor.h"

RamenValueEditor *RamenValueEditor::ofType(enum conf::RamenValueType type, conf::RamenValue const *val, QWidget *parent)
{
  RamenValueEditor *editor = new RamenValueEditor(type, parent);
  editor->setText(val->toQString());
  return editor;
}

conf::RamenValue *RamenValueEditor::getValue() const
{
  return conf::RamenValue::ofQString(type, text());
}
