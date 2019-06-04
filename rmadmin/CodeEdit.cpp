#include "ProgramItem.h"
#include "conf.h"
#include "CodeEdit.h"

CodeEdit::CodeEdit(conf::Key const &key_, QWidget *parent) :
  QTextEdit(parent), key(key_)
{
  setReadOnly(true);

  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  conf::kvs_lock.unlock_shared();

  connect(&kv, &KValue::valueCreated, this, &CodeEdit::setValue);
  connect(&kv, &KValue::valueChanged, this, &CodeEdit::setValue);
  // TODO: valueLocked/valueUnlocked and make this an AtomicForm, cf KLineEdit
  // TODO: valueDeleted.

  if (kv.isSet()) setValue(key, kv.value());
}

void CodeEdit::setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::String const> s =
    std::dynamic_pointer_cast<conf::String const>(v);
  if (! s) {
    std::cout << "CodeEdit: Code text not a string!?" << std::endl;
    return;
  }
  setPlainText(s->s);
}
