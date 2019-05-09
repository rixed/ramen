#ifndef OPERATIONBSITEM_H_190508
#define OPERATIONBSITEM_H_190508
#include <vector>
#include <optional>
#include <QString>
#include <QVariant>
#include <QGraphicsItem>
#include <QGraphicsItemGroup>
#include <QBrush>
#include <QPoint>
#include "confValue.h"
#include "GraphAnchor.h"

class OperationsModel;
class GraphAnchor;

/* OperationsItem is an Item in the OperationsModel *and* in the
 * scene of the GraphView. */

class OperationsItem : public QObject, public QGraphicsItem
{
  Q_OBJECT
  Q_PROPERTY(QPointF pos READ pos WRITE setPos)
  Q_INTERFACES(QGraphicsItem)

  QBrush brush;
  /* All subItems will be children of this one, which in turn is our child
   * node. So to collapse subitems it's enough to subItems.hide() */
  QGraphicsItemGroup subItems;
  bool collapsed;

public:
  QString const name;
  /* We store a pointer to the parents, because no item is ever reparented.
   * When a parent is deleted, it deletes recursively all its children. */
  OperationsItem *treeParent;
  int row;
  GraphAnchor anchorIn;
  GraphAnchor anchorOut;

  OperationsItem(OperationsItem *treeParent, QString const &name, QBrush brush=Qt::NoBrush);
  virtual ~OperationsItem() = 0;
  virtual QVariant data(int) const = 0;
  // Reorder the children after some has been added/removed
  virtual void reorder(OperationsModel const *) {};
  virtual void setProperty(QString const &, std::shared_ptr<conf::Value const>) {};

  // For the GraphView:
  virtual QRectF boundingRect() const;
  virtual void paint(QPainter *painter, const QStyleOptionGraphicsItem *option, QWidget *widget);

  void setCollapsed(bool);
  QString fqName() const;
};

class SiteItem;
class ProgramItem;

#endif
