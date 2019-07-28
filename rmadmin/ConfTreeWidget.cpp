#include <iostream>
#include <QPushButton>
#include <QHBoxLayout>
#include <QHeaderView>
#include "conf.h"
#include "ConfTreeItem.h"
#include "Resources.h"
#include "KLabel.h"
#include "KFloatEditor.h"
#include "ConfTreeWidget.h"

static bool verbose = false;

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

// Slot to propagates editor valueChanged into the item emitDatachanged
void ConfTreeWidget::editedValueChanged(conf::Key const &k, std::shared_ptr<conf::Value const>)
{
  ConfTreeItem *item = itemOfKey(k);
  if (item) item->emitDataChanged();
}

QWidget *ConfTreeWidget::actionWidget(conf::Key const &k, AtomicWidget *editor)
{
  // The widget for the "Actions" column:
  QWidget *widget = new QWidget;
  QHBoxLayout *layout = new QHBoxLayout;
  QPushButton *lockButton = new QPushButton("lock");
  layout->addWidget(lockButton);
  QPushButton *writeButton = new QPushButton("write");
  layout->addWidget(writeButton);
  QPushButton *unlockButton = new QPushButton("unlock");
  layout->addWidget(unlockButton);
  QPushButton *delButton = new QPushButton("delete");
  layout->addWidget(delButton);
  widget->setLayout(layout);

  connect(lockButton, &QPushButton::clicked, this, [k](bool) {
      conf::askLock(k);
  });
  connect(writeButton, &QPushButton::clicked, this, [k, editor](bool) {
      std::shared_ptr<conf::Value const>  v(editor->getValue());
      conf::askSet(k, v);
  });
  connect(unlockButton, &QPushButton::clicked, this, [k](bool) {
      conf::askUnlock(k);
  });
  connect(delButton, &QPushButton::clicked, this, [k](bool) {
      conf::askDel(k);
  });

  return widget;
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
  if (item) {
    if (1 == len) return item;
    return findOrCreateItem(names, k, kv, item);
  }

  // Create it:

  if (! kv) return nullptr;

  item =
    1 == len ?
      new ConfTreeItem(kv, name, parent, nullptr) : // TODO: sort
      new ConfTreeItem(nullptr, name, parent, nullptr); // TODO: sort

  if (! item) return nullptr;

  if (parent)
    parent->addChild(item);
  else
    addTopLevelItem(item);

  if (len > 1) {
    return findOrCreateItem(names, k, kv, item);
  } else {
    AtomicWidget *editor = kv->val->editorWidget(k);
    // Redraw/resize whenever the value is changed:
    connect(editor, &AtomicWidget::valueChanged, this, &ConfTreeWidget::editedValueChanged);

    QWidget *widget = dynamic_cast<QWidget *>(editor);
    assert(widget);
    setItemWidget(item, 1, widget);
    setItemWidget(item, 3, actionWidget(k, editor));
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
  setColumnCount(CONFTREE_WIDGET_NUM_COLUMNS);
  static QStringList labels { "Name", "Value", "Lock", "Actions" };
  setHeaderLabels(labels);
  header()->setStretchLastSection(false);
  header()->setSectionResizeMode(0,QHeaderView::ResizeToContents);
  header()->setSectionResizeMode(1,QHeaderView::Stretch);
  header()->setSectionResizeMode(2,QHeaderView::ResizeToContents);
  header()->setSectionResizeMode(3,QHeaderView::ResizeToContents);

  if (verbose) std::cout << "ConfTreeWidget: Created in thread " << std::this_thread::get_id () << std::endl;

  /* Register to every change in the kvs: */
  conf::autoconnect("", [this](conf::Key const &k, KValue const *kv) {
    /* We'd like to create the item right now, but we are in the wrong thread.
     * In this (Ocaml) thread we must only connect future signals from that
     * kv into the proper slots that will create/update/delete the item. */
    if (verbose) std::cout << "ConfTreeWidget: Connecting to all changes of key " << k.s << " in thread " << std::this_thread::get_id() << std::endl;
    connect(kv, &KValue::valueCreated, this, [this, kv](conf::Key const &k, std::shared_ptr<conf::Value const>, QString const &, double) {
      if (verbose) std::cout << "ConfTreeWidget: Received valueCreated signal in thread " << std::this_thread::get_id() << "!" << std::endl;
      (void)createItem(k, kv);
    });
    /* Better wait for the value viewer/editor to signal it:
    connect(kv, &KValue::valueChanged, this, [this](conf::Key const &k, std::shared_ptr<conf::Value const>, QString const &, double) {
      ConfTreeItem *item = itemOfKey(k);
      if (item) item->emitDataChanged();
    });*/
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
  });
}
