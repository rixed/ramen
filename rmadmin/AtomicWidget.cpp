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
  connect(kvs, &KVStore::keyChanged,
          this, &AtomicWidget::onChange);

  layout = new QStackedLayout;
  layout->setObjectName("AtomicStackedLayout");
  setLayout(layout);
}

void AtomicWidget::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
      case KeyChanged:
        setValueFromStore(change.key, change.kv);
        break;
      case KeyDeleted:
        forgetValue(change.key, change.kv);
        break;
      case KeyLocked:
        lockValue(change.key, change.kv);
        break;
      case KeyUnlocked:
        unlockValue(change.key, change.kv);
        break;
    }
  }
}

void AtomicWidget::relayoutWidget(QWidget *w)
{
  /* Allows a user to relayout several different widgets, and we will switch
   * between them: */
  int const idx(layout->indexOf(w));
  if (idx >= 0) {
    layout->setCurrentIndex(idx);
  } else {
    layout->addWidget(w);
  }
}

bool AtomicWidget::setKey(std::string const &newKey)
{
  if (newKey == key()) return true;

  if (verbose)
    qDebug() << "AtomicWidget[" << QString::fromStdString(key())
             << "]: changing key from" << QString::fromStdString(key())
             << "to " << QString::fromStdString(newKey);

  std::string const oldKey = key();
  saveKey(newKey);
  assert(key() == newKey);  // at least for now

  bool ok(true);

  if (newKey.length() > 0) {
    kvs->lock.lock_shared();
    auto it = kvs->map.find(newKey);
    if (it == kvs->map.end()) {
      if (verbose)
        qDebug() << "AtomicWidget[" << QString::fromStdString(newKey)
                 << "]: ...which is not in the kvs yet";
      setEnabled(false);
    } else {
      ok = setValue(it->first, it->second.val);
      if (verbose)
        qDebug() << "AtomicWidget[" << QString::fromStdString(newKey)
                 << "]: set value to" << *it->second.val << (ok ? " (ok)":" XXXXXX");
      if (ok) {
        setEnabled(it->second.isMine());
      } else {
        setEnabled(false);
      }
    }
    kvs->lock.unlock_shared();
  } else {
    // or set the value to nullptr?
    setEnabled(false);
    return true;
  }

  if (ok) emit keyChanged(oldKey, newKey);

  return ok;
}

void AtomicWidget::lockValue(std::string const &k, KValue const &kv)
{
  if (k != key()) return;

  setEnabled(my_uid && kv.owner.has_value() && kv.owner == *my_uid);
}

/* TODO: Couldn't we have a single lockChange signal, now that both lock
 * and unlock pass a PVPair? */
void AtomicWidget::unlockValue(std::string const &k, KValue const &)
{
  if (k != key()) return;
  setEnabled(false);
}

void AtomicWidget::forgetValue(std::string const &k, KValue const &)
{
  if (k != key()) return;

  if (verbose)
    qDebug() << "AtomicWidget" << this << "forgetValue for key"
             << QString::fromStdString(k);

  setKey(std::string()); // should also disable the widget
}

void AtomicWidget::setValueFromStore(std::string const &k, KValue const &kv)
{
  if (k != key()) return;
  setValue(k, kv.val);
}
