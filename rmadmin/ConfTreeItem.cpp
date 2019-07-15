#include <iostream>
#include <QDateTime>
#include "KValue.h"
#include "Resources.h"
#include "ConfTreeItem.h"

ConfTreeItem::ConfTreeItem(KValue const *kValue_, QString const name_, ConfTreeItem *parent, ConfTreeItem *preceding) :
  QTreeWidgetItem(parent, preceding, UserType),
  name(name_),
  kValue(kValue_) {}

QVariant ConfTreeItem::data(int column, int role) const
{
  if (role == Qt::DecorationRole && column == 1 && kValue) {
    Resources *r = Resources::get();
    return QIcon(kValue->isLocked() ? r->lockedPixmap : r->unlockedPixmap);
  }

  if (role != Qt::DisplayRole) return QVariant();

  if (0 == column) return QVariant(name);

  if (! kValue) return QVariant();

  switch (column) {
    case 1: // lock status
      // k for the lock, and then maybe "locked by ... until ..."
      if (kValue->isLocked()) {
        QDateTime until = QDateTime::fromSecsSinceEpoch(kValue->expiry);
        return QString("locked by ") + *kValue->owner + QString(" until ") + until.toString();
      } else
        return QString("unlocked");

    // TODO: if kValue: build the key, lookup etc.
    default:
      return QVariant();
  }
}
