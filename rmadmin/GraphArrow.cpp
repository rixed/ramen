#include <QPainter>
#include "GraphArrow.h"

GraphArrow::GraphArrow(QGraphicsItem const *from_, QGraphicsItem const *to_, QGraphicsItem *parent) :
  QGraphicsItem(parent), from(from_), to(to_)
{
}

GraphArrow::~GraphArrow() {};

QRectF GraphArrow::boundingRect() const
{
  QPointF const fromPos = from->scenePos();
  QPointF const toPos = to->scenePos();
  return QRectF(fromPos, toPos);
}

void GraphArrow::paint(QPainter *painter, const QStyleOptionGraphicsItem *option, QWidget *)
{
  (void)option;
  QPointF const fromPos = from->scenePos();
  QPointF const toPos = to->scenePos();
  painter->drawLine(fromPos, toPos);
}
