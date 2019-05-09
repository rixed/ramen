#ifndef OPERATIONBSITEM_H_190508
#define OPERATIONBSITEM_H_190508
#include <vector>
#include <optional>
#include <QString>
#include <QVariant>
#include <QGraphicsItem>
#include <QGraphicsItemGroup>
#include <QBrush>
#include "confValue.h"

class OperationsModel;

/* OperationsItem is an Item in the OperationsModel *and* in the
 * scene of the GraphView. */

class OperationsItem : public QGraphicsItem
{
  QBrush brush;
  /* All subItems will be children of this one, which in turn is our child
   * node. So to collapse subitems it's enough to subItems.hide() */
  QGraphicsItemGroup subItems;
  bool collapsed;

  /* We store a pointer to the parents, because no item is ever reparented.
   * When a parent is deleted, it deletes recursively all its children. */
public:
  OperationsItem *parent; // in the tree
  std::vector<OperationsItem *> preds; // in the graph
  int row;
  OperationsItem(OperationsItem *parent, QBrush brush=Qt::NoBrush);
  virtual ~OperationsItem() = 0;
  virtual QVariant data(int) const = 0;
  // Reorder the children after some has been added/removed
  virtual void reorder(OperationsModel const *) {};
  virtual void setProperty(QString const &, std::shared_ptr<conf::Value const>) {};

  // For the GraphView:
  virtual QRectF boundingRect() const;
  virtual void paint(QPainter *painter, const QStyleOptionGraphicsItem *option, QWidget *widget);

  void setCollapsed(bool);
};

class SiteItem;
class ProgramItem;

class FunctionItem : public OperationsItem
{
  std::optional<bool> isUsed;
public:
  QString name;
  FunctionItem(OperationsItem *parent, QString name);
  ~FunctionItem();
  QVariant data(int) const;
  void setProperty(QString const &, std::shared_ptr<conf::Value const>);
};

class ProgramItem : public OperationsItem
{
public:
  QString name;
  // As we are going to point to item from their children we do not want them
  // to move in memory, so let's use a vector of pointers:
  std::vector<FunctionItem *> functions;
  ProgramItem(OperationsItem *parent, QString name);
  ~ProgramItem();
  QVariant data(int) const;
  void reorder(OperationsModel const *);
};

class SiteItem : public OperationsItem
{
public:
  QString name;
  std::optional<bool> isMaster;
  std::vector<ProgramItem *> programs;
  SiteItem(OperationsItem *parent, QString name);
  ~SiteItem();
  QVariant data(int) const;
  void setProperty(QString const &, std::shared_ptr<conf::Value const>);
  void reorder(OperationsModel const *);
};


#endif
