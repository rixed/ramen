#include <iostream>
#include <QPushButton>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QMessageBox>
#include <QKeyEvent>
#include <QStackedLayout>
#include "conf.h"
#include "once.h"
#include "ConfTreeItem.h"
#include "Resources.h"
#include "KFloatEditor.h"
#include "KShortLabel.h"
#include "ConfTreeEditorDialog.h"
#include "ConfTreeWidget.h"

static bool verbose = true;

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

void ConfTreeWidget::deleteClicked(conf::Key const &k)
{
  QMessageBox msg;
  msg.setText(tr("Are you sure?"));
  msg.setInformativeText(
    tr("Key %1 will be lost forever, there is no undo")
      .arg(QString::fromStdString(k.s)));
  msg.setStandardButtons(QMessageBox::Ok | QMessageBox::Cancel);
  msg.setDefaultButton(QMessageBox::Cancel);
  msg.setIcon(QMessageBox::Critical);
  if (QMessageBox::Ok == msg.exec()) conf::askDel(k);
}

void ConfTreeWidget::openEditorWindow(conf::Key const &k, KValue const *kv)
{
  QDialog *editor = new ConfTreeEditorDialog(k, kv);
  editor->show();
}

QWidget *ConfTreeWidget::actionWidget(conf::Key const &k, KValue const *kv)
{
  // The widget for the "Actions" column:
  QWidget *widget = new QWidget;
  QHBoxLayout *layout = new QHBoxLayout;
  layout->setContentsMargins(0, 0, 0, 0);
  widget->setLayout(layout);

  QPushButton *editButton = new QPushButton(kv->can_write ? "edit":"view");
  layout->addWidget(editButton);
  connect(editButton, &QPushButton::clicked, this, [this, k, kv](bool) {
    openEditorWindow(k, kv);
  });

  if (kv->can_del) {
    QPushButton *delButton = new QPushButton("delete");
    layout->addWidget(delButton);
    connect(delButton, &QPushButton::clicked, this, [this, k](bool) {
      deleteClicked(k);
    });
  }

  return widget;
}

/* Same height as the actionWidget, but invisible: */
QWidget *ConfTreeWidget::fillerWidget()
{
  QWidget *widget = new QWidget;
  QHBoxLayout *layout = new QHBoxLayout;
  layout->setContentsMargins(0, 0, 0, 0);
  QPushButton *nopButton = new QPushButton;
  nopButton->setFlat(true);
  nopButton->setEnabled(false);
  layout->addWidget(nopButton);
  widget->setLayout(layout);
  return widget;
}

/* Add a key by adding the ConfTreeItems recursively (or reuse preexisting one),
 * and return the leaf one.
 * Will not create it if kv is null. */
ConfTreeItem *ConfTreeWidget::findOrCreateItem(QStringList &names, conf::Key const &k, KValue const *kv, ConfTreeItem *parent, bool topLevel)
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
      new ConfTreeItem(k, kv, name, parent, nullptr) : // TODO: sort
      new ConfTreeItem(k, nullptr, name, parent, nullptr); // TODO: sort

  if (! item) return nullptr;

  if (parent)
    parent->addChild(item);
  else
    addTopLevelItem(item);

  if (len > 1) {
    /* If that's a top-level item, add a "filler" in the action column in
     * order for the first line of the QTreeWidget (the one used to compute
     * the uniform height) has the same size as the taller ones. */
    if (topLevel) setItemWidget(item, 3, fillerWidget());
    return findOrCreateItem(names, k, kv, item);
  } else {
    KShortLabel *shortLabel = new KShortLabel(this);
    shortLabel->setKey(k);
    shortLabel->setContentsMargins(8, 8, 8, 8);
    // Redraw/resize whenever the value is changed:
    connect(shortLabel, &AtomicWidget::valueChanged,
            this, &ConfTreeWidget::editedValueChanged);
    setItemWidget(item, 1, shortLabel);
    setItemWidget(item, 3, actionWidget(k, kv));
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
  return findOrCreateItem(names, k, kv, nullptr, true);
}

void ConfTreeWidget::activateItem(QTreeWidgetItem *item_, int)
{
  ConfTreeItem *item = dynamic_cast<ConfTreeItem *>(item_);
  if (! item) {
    std::cout << "Activated an item that's not a ConfTreeItem!?" << std::endl;
    return;
  }

  if (item->kValue) {
    openEditorWindow(item->key, item->kValue);
  } else {
    item->setExpanded(! item->isExpanded());
  }
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
  setUniformRowHeights(true);
  setColumnCount(CONFTREE_WIDGET_NUM_COLUMNS);
  static QStringList labels { "Name", "Value", "Lock", "Actions" };
  setHeaderLabels(labels);
  header()->setStretchLastSection(false);
  header()->setSectionResizeMode(0, QHeaderView::ResizeToContents);
  header()->setSectionResizeMode(1, QHeaderView::Stretch);
  header()->setSectionResizeMode(2, QHeaderView::ResizeToContents);
  header()->setSectionResizeMode(3, QHeaderView::ResizeToContents);

  if (verbose) std::cout << "ConfTreeWidget: Created in thread " << std::this_thread::get_id () << std::endl;

  /* Get the activation signal to either collapse/expand or edit: */
  connect(this, &QTreeWidget::itemActivated, this, &ConfTreeWidget::activateItem);

  /* Register to every change in the kvs: */
  conf::autoconnect("", [this](conf::Key const &k, KValue const *kv) {
    /* We'd like to create the item right now, but we are in the wrong thread.
     * In this (Ocaml) thread we must only connect future signals from that
     * kv into the proper slots that will create/update/delete the item. */
    if (verbose) std::cout << "ConfTreeWidget: Connecting to all changes of key " << k.s << " in thread " << std::this_thread::get_id() << std::endl;
    Once::connect(kv, &KValue::valueCreated, this, [this, kv](conf::Key const &k, std::shared_ptr<conf::Value const>, QString const &, double) {
      if (verbose) std::cout << "ConfTreeWidget: Received valueCreated signal in thread " << std::this_thread::get_id() << "!" << std::endl;
      (void)createItem(k, kv);
    });
    /* Better wait for the value viewer/editor to signal it:
    connect(kv, &KValue::valueChanged, this, [this](conf::Key const &k, std::shared_ptr<conf::Value const>, QString const &, double) {
      ConfTreeItem *item = itemOfKey(k);
      if (item) item->emitDataChanged();
    });*/
    connect(kv, &KValue::valueLocked, this, [this](conf::Key const &k, QString const &, double) {
      editedValueChanged(k);
      resizeColumnToContents(2);
    });
    connect(kv, &KValue::valueUnlocked, this, [this](conf::Key const &k) {
      editedValueChanged(k);
      resizeColumnToContents(2);
    });
    connect(kv, &KValue::valueDeleted, this, [this](conf::Key const &k) {
      /* Let's assume the KValue that have sent this signal will be deleted
       * and will never fire again... */
      // Note: no need to emitDataChanged on the parent
      delete itemOfKey(k);
    });
  });
}

void ConfTreeWidget::keyPressEvent(QKeyEvent *event)
{
  QTreeWidget::keyPressEvent(event);

  switch (event->key()) {
    case Qt::Key_Space:
    case Qt::Key_Select:
    case Qt::Key_Enter:
    case Qt::Key_Return:
      if (currentItem()) {
        emit QTreeWidget::itemActivated(currentItem(), 0);
      }
  }
}

QSize ConfTreeWidget::minimumSizeHint() const
{
  return QSize(800, 500);
}
