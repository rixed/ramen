#ifndef CONFTREEWIDGET_H_190715
#define CONFTREEWIDGET_H_190715
#include <QTreeWidget>
#include <QStringList>
#include "confKey.h"

class KValue;
class ConfTreeItem;

class ConfTreeWidget : public QTreeWidget
{
  Q_OBJECT

  ConfTreeItem *findOrCreateItem(QStringList &names, conf::Key const &, KValue const * = nullptr, ConfTreeItem *parent = nullptr);
  ConfTreeItem *createItem(conf::Key const &, KValue const *);
  ConfTreeItem *itemOfKey(conf::Key const &);
  ConfTreeItem *findItem(QString const &name, ConfTreeItem *parent) const;

public:
  ConfTreeWidget(QWidget *parent = nullptr);
};

#endif
