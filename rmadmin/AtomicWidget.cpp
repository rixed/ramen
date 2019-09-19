#include <iostream>
#include <cassert>
#include <QStackedLayout>
#include "conf.h"
#include "confValue.h"
#include "AtomicWidget.h"

static bool const verbose = true;

AtomicWidget::AtomicWidget(QWidget *parent) :
  QWidget(parent)
{
  connect(&kvs, &KVStore::valueCreated,
          this, &AtomicWidget::setValueFromStore);
  connect(&kvs, &KVStore::valueChanged,
          this, &AtomicWidget::setValueFromStore);
  connect(&kvs, &KVStore::valueDeleted,
          this, &AtomicWidget::forgetValue);
  connect(&kvs, &KVStore::valueLocked,
          this, &AtomicWidget::lockValue);
  connect(&kvs, &KVStore::valueUnlocked,
          this, &AtomicWidget::unlockValue);
}

void AtomicWidget::relayoutWidget(QWidget *w)
{
  QStackedLayout *layout = new QStackedLayout;
  layout->addWidget(w);
  setLayout(layout);
}

void AtomicWidget::setKey(std::string const &newKey)
{
  if (newKey == key) return;

  if (verbose)
    std::cout << "AtomicWidget[" << key << "]: changing key from " << key << " to "
              << newKey << std::endl;

  std::string const oldKey = key;
  key = newKey;

  if (key.length() > 0) {
    kvs.lock.lock_shared();
    auto it = kvs.map.find(key);
    if (it == kvs.map.end()) {
      if (verbose)
        std::cout << "AtomicWidget[" << key << "]: ...which is not in the kvs" << std::endl;
      setEnabled(false);
    } else {
      bool const ok = setValue(it->first, it->second.val);
      if (verbose)
        std::cout << "AtomicWidget[" << key << "]: set value to " << *it->second.val << (ok ? " (ok)":" XXXXXX") << std::endl;
      assert(ok);
      setEnabled(it->second.isMine());
    }
    kvs.lock.unlock_shared();
  } else {
    // or set the value to nullptr?
    setEnabled(false);
  }

  emit keyChanged(oldKey, newKey);
}

void AtomicWidget::lockValue(KVPair const &kvp)
{
  if (kvp.first != key) return;

  setEnabled(my_uid && kvp.second.owner.has_value() &&
             kvp.second.owner == *my_uid);
}

/* TODO: Couldn't we have a single lockChange signal, now that both lock
 * and unlock pass a PVPair? */
void AtomicWidget::unlockValue(KVPair const &kvp)
{
  if (kvp.first != key) return;
  setEnabled(false);
}

void AtomicWidget::forgetValue(KVPair const &kvp)
{
  if (kvp.first != key) return;
  setKey(std::string()); // should also disable the widget
}

void AtomicWidget::setValueFromStore(KVPair const &kvp)
{
  if (kvp.first != key) return;
  setValue(kvp.first, kvp.second.val);
}
