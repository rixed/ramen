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

  ConfTreeItem *findOrCreateItem(QStringList &names, conf::Key const &, KValue const * = nullptr, ConfTreeItem *parent = nullptr, bool topLevel = false);
  ConfTreeItem *createItem(conf::Key const &, KValue const *);
  ConfTreeItem *itemOfKey(conf::Key const &);
  ConfTreeItem *findItem(QString const &name, ConfTreeItem *parent) const;

  QWidget *actionWidget(conf::Key const &, KValue const *);
  QWidget *fillerWidget();

public:
  ConfTreeWidget(QWidget *parent = nullptr);
  QSize minimumSizeHint() const override;

protected:
  void keyPressEvent(QKeyEvent *) override;

protected slots:
  void editedValueChanged(conf::Key const &, std::shared_ptr<conf::Value const> v = nullptr);
  void deleteClicked(conf::Key const &);
  void activateItem(QTreeWidgetItem *item, int column);
  void openEditorWindow(conf::Key const &, KValue const *);
};

#endif
