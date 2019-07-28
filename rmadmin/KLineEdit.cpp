#include <iostream>
#include <cassert>
#include "KLineEdit.h"
#include "PosDoubleValidator.h"
#include "PosIntValidator.h"

KLineEdit::KLineEdit(conf::Key const &key, QWidget *parent) :
  AtomicWidget(key, parent)
{
  lineEdit = new QLineEdit(this);
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  if (kv.isSet()) {
    bool ok = setValue(key, kv.val);
    assert(ok); // ?
  }
  setEnabled(kv.isMine());
  conf::kvs_lock.unlock_shared();

  connect(&kv, &KValue::valueCreated, this, &KLineEdit::setValue);
  connect(&kv, &KValue::valueChanged, this, &KLineEdit::setValue);
  connect(&kv, &KValue::valueLocked, this, &KLineEdit::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &KLineEdit::unlockValue);
}

std::shared_ptr<conf::Value const> KLineEdit::getValue() const
{
  VString *v = new VString(lineEdit->text());
  return std::shared_ptr<conf::Value const>(new conf::RamenValueValue(v));
}

void KLineEdit::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  lineEdit->setEnabled(enabled);
}

bool KLineEdit::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString());
  if (new_v != lineEdit->text()) {
    lineEdit->setText(new_v);
    emit valueChanged(k, v);
  }
  return true;
}
