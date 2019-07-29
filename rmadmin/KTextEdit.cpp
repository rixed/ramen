#include <iostream>
#include <cassert>
#include "KTextEdit.h"

KTextEdit::KTextEdit(QString const &sourceName, QWidget *parent) :
  AtomicWidget(conf::Key("sources/" + sourceName.toStdString() + "/ramen"), parent)
{
  textEdit = new QTextEdit;
  setCentralWidget(textEdit);

  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  if (kv.isSet()) {
    bool ok = setValue(key, kv.val);
    assert(ok); // ?
  }
  setEnabled(kv.isMine());
  conf::kvs_lock.unlock_shared();

  connect(&kv, &KValue::valueCreated, this, &KTextEdit::setValue);
  connect(&kv, &KValue::valueChanged, this, &KTextEdit::setValue);
  connect(&kv, &KValue::valueLocked, this, &KTextEdit::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &KTextEdit::unlockValue);
  // TODO: valueDeleted.
}

std::shared_ptr<conf::Value const> KTextEdit::getValue() const
{
  return std::shared_ptr<conf::Value const>(
    new conf::RamenValueValue(new VString(textEdit->toPlainText())));
}

void KTextEdit::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  textEdit->setReadOnly(! enabled);
}

bool KTextEdit::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString());
  if (new_v != textEdit->toPlainText()) {
    textEdit->setPlainText(v->toQString());
    emit valueChanged(k, v);
  }

  return true;
}
