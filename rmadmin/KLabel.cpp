#include "KLabel.h"

KLabel::KLabel(conf::Key const key, bool wordWrap, QWidget *parent) :
  AtomicWidget(key, parent)
{
  label = new QLabel(this);
  label->setWordWrap(wordWrap);

  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  if (kv.isSet()) {
    bool ok = setValue(key, kv.val);
    assert(ok); // ?
  }
  setEnabled(kv.isMine());
  conf::kvs_lock.unlock_shared();

  connect(&kv, &KValue::valueChanged, this, &KLabel::setValue);
}
