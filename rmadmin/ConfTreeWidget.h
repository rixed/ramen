#ifndef CONFTREEWIDGET_H_190715
#define CONFTREEWIDGET_H_190715
#include <QStringList>
#include <QTreeWidget>
#include "conf.h"
#include "confValue.h"

class AtomicWidget;
class ConfTreeItem;
struct KValue;

#define CONFTREE_WIDGET_NUM_COLUMNS 4

class ConfTreeWidget : public QTreeWidget
{
  Q_OBJECT

  void createItemByNames(
    QStringList &, std::string const &, KValue const &, ConfTreeItem * = nullptr, bool = false);
  ConfTreeItem *findItemByNames(QStringList &names, ConfTreeItem * = nullptr);

  ConfTreeItem *itemOfKey(std::string const &);
  ConfTreeItem *findItem(QString const &name, ConfTreeItem *parent) const;

  QWidget *actionWidget(std::string const &, bool, bool);
  QWidget *fillerWidget();

  void createItem(std::string const &, KValue const &);
  void deleteItem(std::string const &, KValue const &);
  void editedValueChangedFromStore(std::string const &, KValue const &kvp);

public:
  ConfTreeWidget(QWidget *parent = nullptr);
  QSize minimumSizeHint() const override;

protected:
  void keyPressEvent(QKeyEvent *) override;

protected slots:
  void onChange(QList<ConfChange> const &);
  void editedValueChanged(std::string const &, std::shared_ptr<conf::Value const>);
  void deleteClicked(std::string const &);
  void activateItem(QTreeWidgetItem *item, int column);
  void openEditorWindow(std::string const &);
};

#endif
