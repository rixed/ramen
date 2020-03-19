#include <cassert>
#include <QDebug>
#include <QStackedLayout>
#include "conf.h"
#include "confValue.h"
#include "AtomicWidget.h"

static bool const verbose(false);

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
    qDebug() << "AtomicWidget[" << QString::fromStdString(key)
             << "]: changing key from" << QString::fromStdString(key)
             << "to " << QString::fromStdString(newKey);

  std::string const oldKey = key;
  key = newKey;

  if (key.length() > 0) {
    kvs.lock.lock_shared();
    auto it = kvs.map.find(key);
    if (it == kvs.map.end()) {
      if (verbose)
        qDebug() << "AtomicWidget[" << QString::fromStdString(key)
                 << "]: ...which is not in the kvs yet";
      setEnabled(false);
    } else {
      bool const ok = setValue(it->first, it->second.val);
      if (verbose)
        qDebug() << "AtomicWidget[" << QString::fromStdString(key)
                 << "]: set value to" << *it->second.val << (ok ? " (ok)":" XXXXXX");
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

void AtomicWidget::lockValue(std::string const &k, KValue const &kv)
{
  if (k != key) return;

  setEnabled(my_uid && kv.owner.has_value() && kv.owner == *my_uid);
}

/* TODO: Couldn't we have a single lockChange signal, now that both lock
 * and unlock pass a PVPair? */
void AtomicWidget::unlockValue(std::string const &k, KValue const &)
{
  if (k != key) return;
  setEnabled(false);
}

void AtomicWidget::forgetValue(std::string const &k, KValue const &)
{
  if (k != key) return;
  setKey(std::string()); // should also disable the widget
}

void AtomicWidget::setValueFromStore(std::string const &k, KValue const &kv)
{
  if (k != key) return;
  setValue(k, kv.val);
}
