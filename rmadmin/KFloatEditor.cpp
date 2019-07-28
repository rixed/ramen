#include <cassert>
#include "KFloatEditor.h"

KFloatEditor::KFloatEditor(std::string const &key, double min, double max, QWidget *parent) :
  QLineEdit(parent),
  AtomicWidget(key)
{
  (void)min; (void)max; // TODO
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
  VFloat *v = new VFloat(text().toDouble());
  return std::shared_ptr<conf::Value const>(new conf::RamenValueValue(v));
}

void KFloatEditor::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  QLineEdit::setEnabled(enabled);
}

/* TODO: returning an actual error message that could be used in the error
 * label would be better */
bool KFloatEditor::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  // Retrieve the VFloat (or fail)
  std::shared_ptr<conf::RamenValueValue const> rvv =
    std::dynamic_pointer_cast<conf::RamenValueValue const>(v);
  // TODO: a failing editor should be hidden and replaced with an error label
  if (! rvv) return false;

  std::shared_ptr<VFloat const> rv =
    std::dynamic_pointer_cast<VFloat const>(rvv->v);
  if (! rv) return false;

  QString new_v(rv->toQString());
  if (new_v != text()) {
    QLineEdit::setText(new_v);
    emit valueChanged(k, v);
  }

  return true;
}
