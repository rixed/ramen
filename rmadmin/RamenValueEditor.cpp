#include "RamenType.h"
#include "RamenValueEditor.h"

RamenValueEditor *RamenValueEditor::ofType(std::shared_ptr<RamenType const> type, RamenValue const *val, QWidget *parent)
{
  RamenValueEditor *editor = new RamenValueEditor(type, parent);
  editor->setText(val->toQString(std::string()));
  return editor;
}

RamenValue *RamenValueEditor::getValue() const
{
  return type->vtyp->valueOfQString(text());
}
