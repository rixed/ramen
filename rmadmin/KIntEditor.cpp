#include <cassert>
#include <limits>
#include "RangeIntValidator.h"
#include "KIntEditor.h"

KIntEditor::KIntEditor(
    std::function<RamenValue *(QString const &)> ofQString_,
    QWidget *parent,
    std::optional<int128_t> min,
    std::optional<int128_t> max) :
  AtomicWidget(parent),
  ofQString(ofQString_)
{
  lineEdit = new QLineEdit;
  relayoutWidget(lineEdit);

  int imin =
    min && *min >= std::numeric_limits<int>::min() ?
      *min : std::numeric_limits<int>::min();
  int imax =
    max && *max <= std::numeric_limits<int>::max() ?
      *max : std::numeric_limits<int>::max();
  lineEdit->setValidator(RangeIntValidator::forRange(imin, imax));

  connect(lineEdit, &QLineEdit::editingFinished,
          this, &KIntEditor::inputChanged);
}

std::shared_ptr<conf::Value const> KIntEditor::getValue() const
{
  RamenValue *v = ofQString(lineEdit->text());
  return std::shared_ptr<conf::Value const>(new conf::RamenValueValue(v));
}

void KIntEditor::setEnabled(bool enabled)
{
  lineEdit->setEnabled(enabled);
}

/* TODO: returning an actual error message that could be used in the error
 * label would be better */
bool KIntEditor::setValue(
  std::string const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString(k));

  if (new_v != lineEdit->text()) {
    lineEdit->setText(new_v);
    emit valueChanged(k, v);
  }

  return true;
}

#undef MakeKIntEditor
