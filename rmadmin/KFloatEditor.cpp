#include <cassert>
#include "RangeDoubleValidator.h"
#include "KFloatEditor.h"

KFloatEditor::KFloatEditor(QWidget *parent, double min, double max) :
  AtomicWidget(parent)
{
  lineEdit = new QLineEdit;
  lineEdit->setValidator(RangeDoubleValidator::forRange(min, max));
  relayoutWidget(lineEdit);
  connect(lineEdit, &QLineEdit::editingFinished,
          this, &KFloatEditor::inputChanged);
}

std::shared_ptr<conf::Value const> KFloatEditor::getValue() const
{
  VFloat *v = new VFloat(lineEdit->text().toDouble());
  return std::shared_ptr<conf::Value const>(new conf::RamenValueValue(v));
}

void KFloatEditor::setEnabled(bool enabled)
{
  lineEdit->setEnabled(enabled);
}

/* TODO: returning an actual error message that could be used in the error
 * label would be better */
bool KFloatEditor::setValue(
  std::string const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString(k));

  if (new_v != lineEdit->text()) {
    lineEdit->setText(new_v);
    emit valueChanged(k, v);
  }

  return true;
}
