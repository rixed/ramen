#ifndef GRAPHARROW_H_190509
#define GRAPHARROW_H_190509
#include <vector>
#include <QGraphicsItem>
#include <QRectF>

class GraphViewSettings;
class QPainterPath;

enum Direction { Right, Left, Up, Down };

class GraphArrow : public QGraphicsItem
{
  struct Line {
    Direction dir;
    int x, y;

    QPointF start(GraphViewSettings const *) const;
    QPointF stop(GraphViewSettings const *) const;
  };
  unsigned channel;
  QColor color;

  // FIXME: shared_ptr:
  GraphViewSettings const *settings;
  QPainterPath arrowPath;
  QPainterPath arrowHead;
  QRectF boundingBox;

public:
  // Because of transparency we cannot rely on arrows being overpaint by
  // above layers. So they should be given the horiz-margin to apply by the
  // constructor.
  GraphArrow(
    GraphViewSettings const *, int x0, int y0, int hmargin0,
    int x1, int y1, int hmargin1, unsigned channel, QColor,
    QGraphicsItem *parent = nullptr);

  QRectF boundingRect() const override;
  void paint(QPainter *, const QStyleOptionGraphicsItem *, QWidget *) override;
};

#endif
