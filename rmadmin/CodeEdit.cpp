#include "ProgramItem.h"
#include "conf.h"
#include "CodeEdit.h"

CodeEdit::CodeEdit(ProgramItem const *p_, QWidget *parent) :
  QTextEdit(parent), p(p_)
{
  setReadOnly(true);

  std::string key("programs/" + p->name.toStdString() + "/source/text");
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  conf::kvs_lock.unlock_shared();

  QObject::connect(&kv, &KValue::valueCreated, this, &CodeEdit::setValue);
  QObject::connect(&kv, &KValue::valueChanged, this, &CodeEdit::setValue);
  // TODO: valueLocked/valueUnlocked and make this an AtomicForm, cf KLineEdit
  // TODO: valueDeleted.

  if (kv.isSet()) setValue(key, kv.value());
}

void CodeEdit::setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::String const>s =
    std::dynamic_pointer_cast<conf::String const>(v);
  if (! s) {
    std::cout << "CodeEdit: Code text not a string!?" << std::endl;
    return;
  }
  setPlainText(s->s);
}
