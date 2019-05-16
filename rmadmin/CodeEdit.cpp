#include "ProgramItem.h"
#include "conf.h"
#include "CodeEdit.h"

CodeEdit::CodeEdit(ProgramItem const *p_, QWidget *parent) :
  QTextEdit(parent), p(p_)
{
  setReadOnly(true);

  std::string k("programs/" + p->name.toStdString() + "/source/text");
  std::cout << "CodeEdit: autoconnect to " << k << std::endl;
  conf::autoconnect(k, [this](conf::Key const &, KValue const *kv) {
      std::cout << "CodeEdit: CB called!" << std::endl;
      QObject::connect(kv, &KValue::valueCreated, this, &CodeEdit::setText);
      QObject::connect(kv, &KValue::valueChanged, this, &CodeEdit::setText);
      // TODO: valueLocked/valueUnlocked and make this an AtomicForm.
      // TODO: valueDeleted.
    }
  );
}

CodeEdit::~CodeEdit() {}

void CodeEdit::setText(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  std::cout << "CodeEdit: setText called!" << std::endl;
  std::shared_ptr<conf::String const>s =
    std::dynamic_pointer_cast<conf::String const>(v);
  if (! s) {
    std::cout << "CodeEdit: Code text not a string!?" << std::endl;
    return;
  }
  setPlainText(s->s);
}
