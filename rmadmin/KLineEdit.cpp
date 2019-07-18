#include <iostream>
#include "KLineEdit.h"
#include "PosDoubleValidator.h"
#include "PosIntValidator.h"

KLineEdit::KLineEdit(std::string const &key, conf::ValueType valueType_, QWidget *parent) :
  QLineEdit(parent),
  AtomicWidget(key),
  valueType(valueType_)
{
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  conf::kvs_lock.unlock_shared();
  connect(&kv, &KValue::valueCreated, this, &KLineEdit::setValue);
  connect(&kv, &KValue::valueChanged, this, &KLineEdit::setValue);
  connect(&kv, &KValue::valueLocked, this, &KLineEdit::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &KLineEdit::unlockValue);

  if (kv.isSet()) setValue(key, kv.val);
}

std::shared_ptr<conf::Value const> KLineEdit::getValue() const
{
  return std::shared_ptr<conf::Value const>(conf::valueOfQString(valueType, text()));
}

void KLineEdit::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  QLineEdit::setEnabled(enabled);
}

void KLineEdit::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString());
  if (new_v != text()) {
    QLineEdit::setText(new_v);
    emit valueChanged(k, v);
  }
}
