#ifndef CONFTREEITEM_H_190715
#define CONFTREEITEM_H_190715
#include <QTreeWidgetItem>
#include <QString>
#include <QPixmap>

class KValue;

class ConfTreeItem : public QTreeWidgetItem
{
  friend class ConfTreeWidget;  // for calling emitDataChanged

public:
  /* While we already have a map of keys to values in conf::kvs, it is
   * organized as a prefix-tree (still TODO BTW) which is the best structure
   * for matching arbitrary globs, whereas For the ConfTreeview we need a
   * tree that's split at the slash borders (either from between key names or
   * path components names in program or source names).
   * Therefore we build yet another representation of the tree of keys as a
   * simple tree of strings. */
  QString const name;
  // Assumed the KValue will outlive us in the kvs
  KValue const *kValue;

  ConfTreeItem(KValue const *kValue, QString const, ConfTreeItem *parent = nullptr, ConfTreeItem *preceding = nullptr);

  ~ConfTreeItem() {}

  QVariant data(int column, int role) const;
};

#endif
