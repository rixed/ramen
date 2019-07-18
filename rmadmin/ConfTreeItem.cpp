#include <iostream>
#include <QDateTime>
#include "KValue.h"
#include "Resources.h"
#include "ConfTreeWidget.h" // for CONFTREE_WIDGET_NUM_COLUMNS
#include "ConfTreeItem.h"

ConfTreeItem::ConfTreeItem(KValue const *kValue_, QString const name_, ConfTreeItem *parent, ConfTreeItem *preceding) :
  QTreeWidgetItem(parent, preceding, UserType),
  name(name_),
  kValue(kValue_) {}

QVariant ConfTreeItem::data(int column, int role) const
{
  assert(column < CONFTREE_WIDGET_NUM_COLUMNS);

  if (role == Qt::DecorationRole && column == 2 && kValue) {
    Resources *r = Resources::get();
    return QIcon(kValue->isLocked() ? r->lockedPixmap : r->unlockedPixmap);
  }

  if (role != Qt::DisplayRole) return QVariant();

  if (0 == column) return QVariant(name);

  if (! kValue) return QVariant();

  switch (column) {
    case 2: // lock status
      // k for the lock, and then maybe "locked by ... until ..."
      if (kValue->isLocked()) {
        return QString("locked by ") + *kValue->owner +
               QString(" until ") + stringOfDate(kValue->expiry);
      } else {
        return QString("unlocked");
      }
    // Column 1 is the view/edit widget that's set once and for all at creation time
    default:
      return QVariant();
  }
}
