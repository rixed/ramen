#ifndef CONFTREEWIDGET_H_190715
#define CONFTREEWIDGET_H_190715
#include <QTreeWidget>
#include <QStringList>
#include "confKey.h"

class KValue;
class ConfTreeItem;

#define CONFTREE_WIDGET_NUM_COLUMNS 4

class ConfTreeWidget : public QTreeWidget
{
  Q_OBJECT

  ConfTreeItem *findOrCreateItem(QStringList &names, conf::Key const &, KValue const * = nullptr, ConfTreeItem *parent = nullptr);
  ConfTreeItem *createItem(conf::Key const &, KValue const *);
  ConfTreeItem *itemOfKey(conf::Key const &);
  ConfTreeItem *findItem(QString const &name, ConfTreeItem *parent) const;

  QWidget *editorWidget(conf::Key const &);
  QWidget *actionWidget(conf::Key const &);

public:
  ConfTreeWidget(QWidget *parent = nullptr);
};

#endif
