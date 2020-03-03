#ifndef GRAPHITEM_H_190508
#define GRAPHITEM_H_190508
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <QBrush>
#include <QGraphicsItem>
#include <QGraphicsItemGroup>
#include <QPoint>
#include <QString>
#include <QVariant>
#include "confValue.h"
#include "GraphViewSettings.h"

class GraphModel;

/* GraphItem is an Item in the GraphModel *and* in the scene of the GraphView.
 * This is the address of the GraphItem that is stored in QModelIndex
 * internalPointers. It's inherited by actual SiteItem, ProgramItem and
 * FunctionItem, each implementing its own rendering functions.
 *
 * But as we'd like the actual data regarding ramen sites, programs and
 * functions to be shared around and outlive the GraphItem then we'd rathe have
 * that data allocated separately and accessed via a shared_ptr.
 *
 * Therefore, SiteItems, ProgramItems and FunctionItems allocate their data
 * structures (humbly named Site, Program and Function) on the side in
 * shared. */

struct GraphData
{
  QString const name;

  GraphData(QString const &name_) : name(name_) {}

  /* GraphData is going to be inherited: */
  virtual ~GraphData() {};
};

// Note: QGraphicsItem does not inherit QObject
class GraphItem : public QObject, public QGraphicsItem
{
  Q_OBJECT
  Q_PROPERTY(QPointF pos READ pos WRITE setPos)
  Q_PROPERTY(qreal border READ border WRITE setBorder)
  Q_INTERFACES(QGraphicsItem)

  qreal border_;
  QBrush brush;
  /* All subItems will be children of this one, which in turn is our child
   * node. So to collapse subitems it's enough to subItems.hide() */
  QGraphicsItem *subItems;
  bool collapsed;

  void paintLabels(QPainter *, std::vector<std::pair<QString const, QString const>> const &, int y);
  QRect labelsBoundingRect(std::vector<std::pair<QString const, QString const>> const &) const;

protected:
  GraphViewSettings const *settings;

  // Displayed in the graph:
  // TODO: use QStaticText
  virtual std::vector<std::pair<QString const, QString const>> labels() const  = 0;

  // to update parent's frame bbox:
  QVariant itemChange(QGraphicsItem::GraphicsItemChange, const QVariant &);

public:
  int x0, y0, x1, y1;  // in the function grid (absolute!)
  int xRank, yRank;  // the column / row within that column (used for laying out)
  std::set<GraphItem *> parentOps; // also used during layout
  /* We store a pointer to the parents, because no item is ever reparented. */
  GraphItem *treeParent;
  int row;

  std::shared_ptr<GraphData> shared;

  GraphItem(
    GraphItem *treeParent, std::unique_ptr<GraphData> data,
    GraphViewSettings const *);

  int columnCount() const;
  virtual QVariant data(int col, int role) const;
  // Reorder the children after some has been added/removed
  virtual void reorder(GraphModel const *) {};
  virtual void setProperty(QString const &, std::shared_ptr<conf::Value const>) {};
  QModelIndex index(GraphModel const *, int) const;
  bool isCollapsed() const;
  virtual bool isTopHalf() const = 0;

  // For the GraphView:
  QRectF boundingRect() const;
  virtual void paint(QPainter *painter, const QStyleOptionGraphicsItem *option, QWidget *widget);
  // The box representing the operation, regardless of its border (unlike boundingRect):
  virtual QRectF operationRect() const = 0;

  void setCollapsed(bool);
  QString fqName() const;
  QColor color() const;
  qreal border() const;
  void setBorder(qreal);
};

#endif
