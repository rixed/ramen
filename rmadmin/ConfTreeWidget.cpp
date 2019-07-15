#include <iostream>
#include <QPushButton>
#include <QHBoxLayout>
#include "conf.h"
#include "ConfTreeItem.h"
#include "Resources.h"
#include "ConfTreeWidget.h"

ConfTreeItem *ConfTreeWidget::findItem(QString const &name, ConfTreeItem *parent) const
{
  if (parent) {
    // TODO: once sorted, early exit (which also tells where to insert the new one)
    for (int c = 0; c < parent->childCount(); c ++) {
      ConfTreeItem *item = dynamic_cast<ConfTreeItem *>(parent->child(c));
      if (item->name == name) return item;
    }
  } else {
    for (int c = 0; c < topLevelItemCount(); c ++) {
      ConfTreeItem *item = dynamic_cast<ConfTreeItem *>(topLevelItem(c));
      if (item->name == name) return item;
    }
  }

  return nullptr;
}

/* Add a key by adding the ConfTreeItems recursively (or reuse preexisting one),
 * and return the leaf one.
 * Will not create it if kv is null. */
ConfTreeItem *ConfTreeWidget::findOrCreateItem(QStringList &names, conf::Key const &k, KValue const *kv, ConfTreeItem *parent)
{
  int const len = names.count();
  assert(len >= 1);
  QString const name = names.takeFirst();

  ConfTreeItem *item = findItem(name, parent);

  if (! item && kv) {
    item =
      1 == len ?
        new ConfTreeItem(kv, name, parent, nullptr) : // TODO: sort
        new ConfTreeItem(nullptr, name, parent, nullptr); // TODO: sort
  }

  if (! item) return nullptr;

  if (parent)
    parent->addChild(item);
  else
    addTopLevelItem(item);

  if (len > 1)
    return findOrCreateItem(names, k, kv, item);
  else {
    QWidget *widget = new QWidget;
    QHBoxLayout *layout = new QHBoxLayout;
    QPushButton *lockButton = new QPushButton("lock");
    layout->addWidget(lockButton);
    QPushButton *unlockButton = new QPushButton("unlock");
    layout->addWidget(unlockButton);
    QPushButton *delButton = new QPushButton("delete");
    layout->addWidget(delButton);
    widget->setLayout(layout);
    setItemWidget(item, 2, widget);

    connect(lockButton, &QPushButton::clicked, this, [k](bool) {
        conf::askLock(k);
    });
    connect(unlockButton, &QPushButton::clicked, this, [k](bool) {
        conf::askUnlock(k);
    });
    connect(delButton, &QPushButton::clicked, this, [k](bool) {
        conf::askDel(k);
    });
    return item;
  }
}

ConfTreeItem *ConfTreeWidget::createItem(conf::Key const &k, KValue const *kv)
{
  /* We have a new key.
   * Add it to the tree and connect any value change for that value to a
   * slot that will retrieve the item and call it's emitDataChanged function
   * (which will itself call the underlying model to signal a change).
   */
  QString keyName = QString::fromStdString(k.s);
  QStringList names = keyName.split("/", QString::SkipEmptyParts);
  return findOrCreateItem(names, k, kv);
}

ConfTreeItem *ConfTreeWidget::itemOfKey(conf::Key const &k)
{
  QString keyName = QString::fromStdString(k.s);
  QStringList names = keyName.split("/", QString::SkipEmptyParts);
  return findOrCreateItem(names, k);
}

ConfTreeWidget::ConfTreeWidget(QWidget *parent) :
  QTreeWidget(parent)
{
  setColumnCount(3); // TODO: the other columns
  setHeaderLabels({ "Name", "Lock", "Actions" });

  std::cout << "ConfTreeWidget: Created in thread " << std::this_thread::get_id () << std::endl;

  /* Register to every change in the kvs: */
  conf::autoconnect("", [this](conf::Key const &k, KValue const *kv) {
    /* We'd like to create the item right now, but we are in the wrong thread.
     * In this (Ocaml) thread we must only connect future signals from that
     * kv into the proper slots that will create/update/delete the item. */
    std::cout << "ConfTreeWidget: Connecting to all changes of key " << k.s << " in thread " << std::this_thread::get_id() << std::endl;
    connect(kv, &KValue::valueCreated, this, [this, kv](conf::Key const &k, std::shared_ptr<conf::Value const>, QString const &, double) {
      std::cout << "ConfTreeWidget: Received valueCreated signal in thread " << std::this_thread::get_id() << "!" << std::endl;
      (void)createItem(k, kv);
    });
    connect(kv, &KValue::valueChanged, this, [this](conf::Key const &k, std::shared_ptr<conf::Value const>, QString const &, double) {
      ConfTreeItem *item = itemOfKey(k);
      if (item) item->emitDataChanged();
    });
    connect(kv, &KValue::valueChanged, this, [this](conf::Key const &k, std::shared_ptr<conf::Value const>, QString const &, double) {
      ConfTreeItem *item = itemOfKey(k);
      if (item) item->emitDataChanged();
    });
    connect(kv, &KValue::valueLocked, this, [this](conf::Key const &k, QString const &, double) {
      ConfTreeItem *item = itemOfKey(k);
      if (item) item->emitDataChanged();
    });
    connect(kv, &KValue::valueUnlocked, this, [this](conf::Key const &k) {
      ConfTreeItem *item = itemOfKey(k);
      if (item) item->emitDataChanged();
    });
    connect(kv, &KValue::valueDeleted, this, [this](conf::Key const &k) {
      /* Let's assume the KValue that have sent this signal will be deleted
       * and will never fire again... */
      // Note: no need to emitDataChanged on the parent
      delete itemOfKey(k);
    });
    std::cout << "ConfTreeWidget: finished connecting ConfTreeWidget" << std::endl;
  });
}
