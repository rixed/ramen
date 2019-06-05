#include <iostream>
#include "KTextEdit.h"

KTextEdit::KTextEdit(std::string const key, QWidget *parent) :
  QTextEdit(parent), AtomicWidget(key)
{
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  conf::kvs_lock.unlock_shared();
  connect(&kv, &KValue::valueCreated, this, &KTextEdit::setValue);
  connect(&kv, &KValue::valueChanged, this, &KTextEdit::setValue);
  connect(&kv, &KValue::valueLocked, this, &KTextEdit::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &KTextEdit::unlockValue);
  // TODO: valueDeleted.
  if (kv.isSet()) setValue(key, kv.value());
}

std::shared_ptr<conf::Value const> KTextEdit::getValue() const
{
  return std::shared_ptr<conf::Value const>(new conf::String(toPlainText()));
}

void KTextEdit::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  QTextEdit::setReadOnly(! enabled);
}

void KTextEdit::setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  QTextEdit::setText(v->toQString());
}
