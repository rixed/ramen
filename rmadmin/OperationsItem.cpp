#include <iostream>
#include <QPainter>
#include "OperationsItem.h"
#include "OperationsModel.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "SiteItem.h"

// Notice that we use our parent's subItems as the parent of the GraphItem,
// meaning all coordinates will be relative to that parent. Easier when it
// moves.
// Note also that we must initialize row with an invalid value so that
// reorder detect that it's indeed a new value when we insert the first one!
OperationsItem::OperationsItem(OperationsItem *parent_, QString const &name_, QBrush brush_) :
  QGraphicsItem(parent_ ?
      static_cast<QGraphicsItem *>(&parent_->subItems) :
      static_cast<QGraphicsItem *>(parent_)),
  brush(brush_),
  subItems(this),
  collapsed(true),
  name(name_),
  parent(parent_),
  row(-1)
{
  // TreeView is initially collapsed, and so are we:
  subItems.hide();
}

OperationsItem::~OperationsItem() {}

QRectF OperationsItem::boundingRect() const
{
  return QRectF(QPointF(-10, -10), QSizeF(20, 20));
}

void OperationsItem::paint(QPainter *painter, const QStyleOptionGraphicsItem *option, QWidget *widget)
{
  (void)option; (void)widget;
  QPen pen = QPen(Qt::darkBlue);
  pen.setWidthF(2);
  painter->setPen(pen);
  painter->setBrush(brush);
  painter->drawRoundedRect(boundingRect(), 0.1, 0.1);
  painter->drawText(0, 0, QString::number(row));
}

void OperationsItem::setCollapsed(bool c)
{
  collapsed = c;
  subItems.setVisible(!c);
}

QString OperationsItem::fqName() const
{
  if (! parent) return name;
  return parent->fqName() + "/" + name;
}
