#ifndef CONFTREEWIDGET_H_190715
#define CONFTREEWIDGET_H_190715
#include <QTreeWidget>
#include <QStringList>
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

public:
  ConfTreeWidget(QWidget *parent = nullptr);
  QSize minimumSizeHint() const override;

protected:
  void keyPressEvent(QKeyEvent *) override;

protected slots:
  void createItem(std::string const &, KValue const &);
  void editedValueChanged(std::string const &, std::shared_ptr<conf::Value const>);
  void editedValueChangedFromStore(std::string const &, KValue const &kvp);
  void deleteItem(std::string const &, KValue const &);
  void deleteClicked(std::string const &);
  void activateItem(QTreeWidgetItem *item, int column);
  void openEditorWindow(std::string const &);
};

#endif
