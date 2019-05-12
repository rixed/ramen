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
#include "GraphViewSettings.h"

class OperationsModel;

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

  void paintLabels(QPainter *, std::vector<std::pair<QString const, QString const>> const &);
  QRect labelsBoundingRect(std::vector<std::pair<QString const, QString const>> const &) const;

protected:
  GraphViewSettings const *settings;

  // Displayed in the graph:
  // TODO: use QStaticText
  virtual void addLabels(std::vector<std::pair<QString const, QString const>> *) const {};

  // to update parent's frame bbox:
  QVariant itemChange(QGraphicsItem::GraphicsItemChange, const QVariant &);

public:
  int x0, y0, x1, y1;  // in the function grid (absolute!)
  QString const name;
  /* We store a pointer to the parents, because no item is ever reparented.
   * When a parent is deleted, it deletes recursively all its children. */
  OperationsItem *treeParent;
  int row;

  OperationsItem(OperationsItem *treeParent, QString const &name, GraphViewSettings const *, unsigned paletteSize);
  virtual ~OperationsItem() = 0;
  virtual QVariant data(int) const = 0;
  // Reorder the children after some has been added/removed
  virtual void reorder(OperationsModel const *) {};
  virtual void setProperty(QString const &, std::shared_ptr<conf::Value const>) {};

  // For the GraphView:
  virtual QRectF boundingRect() const = 0;
  virtual void paint(QPainter *painter, const QStyleOptionGraphicsItem *option, QWidget *widget);

  void setCollapsed(bool);
  QString fqName() const;
  QColor color() const;

public slots:
  void updateFrame();
};

class SiteItem;
class ProgramItem;

#endif
