#include <iostream>
#include <cassert>
#include "KErrorMsg.h"

/* Beware:
 * First, this setKey is not the one from an AtomicWidget.
 * Second, and more importantly, this can (and will) be called before the key
 * is present in kvs! */
void KErrorMsg::setKey(conf::Key const &key)
{
  assert(! keyIsSet);
  keyIsSet = true;
  std::cout << "KErrorMsg: setting key to " << key << std::endl;

  conf::kvs_lock.lock_shared();
  /* Work around the terrible kvs that's yet to be turned into a proper
   * prefix tree (TODO): */
  if (! conf::kvs.contains(key)) conf::kvs[key].key = key;
  KValue &kv = conf::kvs[key].kv;
  conf::kvs_lock.unlock_shared();

  connect(&kv, &KValue::valueChanged, this, &KErrorMsg::setValue);
  if (kv.isSet()) setValue(key, kv.val);
}

void KErrorMsg::setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::Error const> err =
    std::dynamic_pointer_cast<conf::Error const>(v);
  assert(err);
  QLabel::setStyleSheet(
    err->msg.length() == 0 ?
      "" :
      "background-color: pink");
  QLabel::setText(QString::fromStdString(err->msg));
}
