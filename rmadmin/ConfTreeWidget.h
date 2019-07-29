#ifndef CONFTREEWIDGET_H_190715
#define CONFTREEWIDGET_H_190715
#include <QTreeWidget>
#include <QStringList>
#include "confKey.h"
#include "confValue.h"

class KValue;
class ConfTreeItem;
class AtomicWidget;

#define CONFTREE_WIDGET_NUM_COLUMNS 4

class ConfTreeWidget : public QTreeWidget
{
  Q_OBJECT

  ConfTreeItem *findOrCreateItem(QStringList &names, conf::Key const &, KValue const * = nullptr, ConfTreeItem *parent = nullptr);
  ConfTreeItem *createItem(conf::Key const &, KValue const *);
  ConfTreeItem *itemOfKey(conf::Key const &);
  ConfTreeItem *findItem(QString const &name, ConfTreeItem *parent) const;

  QWidget *actionWidget(conf::Key const &, KValue const *);

public:
  ConfTreeWidget(QWidget *parent = nullptr);

public slots:
  void editedValueChanged(conf::Key const &, std::shared_ptr<conf::Value const> v = nullptr);
  void deleteClicked(conf::Key const &);

};

#endif
