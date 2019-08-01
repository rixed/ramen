#include "once.h"
#include "confValue.h"
#include "conf.h"
#include "KLabel.h"

KLabel::KLabel(conf::Key const &key, QWidget *parent, bool wordWrap) :
  AtomicWidget(key, parent)
{
  label = new QLabel;
  label->setWordWrap(wordWrap);
  setCentralWidget(label);

  SET_INITIAL_VALUE;

  Once::connect(&kv, &KValue::valueCreated, this, &KLabel::setValue);
  connect(&kv, &KValue::valueChanged, this, &KLabel::setValue);
}

bool KLabel::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString(k));

  if (new_v != label->text()) {
    label->setText(new_v);
    emit valueChanged(k, v);
  }

  return true;
}
