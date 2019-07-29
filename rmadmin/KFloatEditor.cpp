#include <cassert>
#include "RangeDoubleValidator.h"
#include "KFloatEditor.h"

KFloatEditor::KFloatEditor(conf::Key const &key, QWidget *parent, double min, double max) :
  AtomicWidget(key, parent)
{
  lineEdit = new QLineEdit;
  lineEdit->setValidator(RangeDoubleValidator::forRange(min, max));
  setCentralWidget(lineEdit);

  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  if (kv.isSet()) {
    bool ok = setValue(key, kv.val);
    assert(ok); // ?
  }
  setEnabled(kv.isMine());
  conf::kvs_lock.unlock_shared();

  connect(&kv, &KValue::valueCreated, this, &KFloatEditor::setValue);
  connect(&kv, &KValue::valueChanged, this, &KFloatEditor::setValue);
  connect(&kv, &KValue::valueLocked, this, &KFloatEditor::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &KFloatEditor::unlockValue);
}

std::shared_ptr<conf::Value const> KFloatEditor::getValue() const
{
  VFloat *v = new VFloat(lineEdit->text().toDouble());
  return std::shared_ptr<conf::Value const>(new conf::RamenValueValue(v));
}

void KFloatEditor::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  lineEdit->setEnabled(enabled);
}

/* TODO: returning an actual error message that could be used in the error
 * label would be better */
bool KFloatEditor::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString(k));
  if (new_v != lineEdit->text()) {
    lineEdit->setText(new_v);
    emit valueChanged(k, v);
  }

  return true;
}
