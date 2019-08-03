#include <cassert>
#include <QStackedLayout>
#include "AtomicWidget.h"

void AtomicWidget::setEnabled(bool enabled)
{
  if (enabled && !last_enabled) {
    // Capture the value at the beginning of edition:
    initValue = getValue();
  }
  last_enabled = enabled;
}

void AtomicWidget::setCentralWidget(QWidget *w)
{
  QStackedLayout *layout = new QStackedLayout;
  layout->addWidget(w);
  setLayout(layout);
}

void AtomicWidget::setKey(conf::Key const &newKey)
{
  std::cout << "AtomicWidget: changing key from " << key << " to " << newKey << std::endl;

  if (newKey == key) return;

  KValue *kv = nullptr;

  /* First disconnect: */
  if (key != conf::Key::null) {
    conf::kvs_lock.lock_shared();
    kv = &conf::kvs[key];
    conf::kvs_lock.unlock_shared();
    /* This also disconnect whatever other signals the inerited implementer
     * might have set: */
    disconnect(kv, 0, this, 0);
  }

  conf::Key const oldKey = key;
  key = newKey;

  if (key != conf::Key::null) {
    conf::kvs_lock.lock_shared();
    kv = &conf::kvs[key];
    if (kv->isSet()) {
      bool ok = setValue(key, kv->val);
      assert(ok);
    }
    extraConnections(kv);
    setEnabled(kv->isMine());
    conf::kvs_lock.unlock_shared();
  } else {
    setEnabled(false);
  }

  emit keyChanged(oldKey, newKey);
}
