#include <QPainter>
#include "OperationsItem.h"
#include "OperationsModel.h"

// Notice that we use our parent's subItems as the parent of the GraphItem,
// meaning all coordinates will be relative to that parent. Easier when it
// moves.
// Note also that we must initialize row with an invalid value so that
// reorder detect that it's indeed a new value when we insert the first one!
OperationsItem::OperationsItem(OperationsItem *parent_, QBrush brush_) :
  QGraphicsItem(parent_ ?
      static_cast<QGraphicsItem *>(&parent_->subItems) :
      static_cast<QGraphicsItem *>(parent_)),
  brush(brush_),
  subItems(this),
  collapsed(true),
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

FunctionItem::FunctionItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent, Qt::blue), name(name_) {}

FunctionItem::~FunctionItem() {}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

void FunctionItem::setProperty(QString const &p, std::shared_ptr<conf::Value const> v)
{
  if (p == "is_used") {
    std::shared_ptr<conf::Bool const> b = std::dynamic_pointer_cast<conf::Bool const>(v);
    if (b) isUsed = b->b;
  }
}

ProgramItem::ProgramItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent, Qt::red), name(name_) {}

ProgramItem::~ProgramItem()
{
  for (FunctionItem *function : functions) {
    delete function;
  }
}

QVariant ProgramItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

void ProgramItem::reorder(OperationsModel const *model)
{
  for (int i = 0; (size_t)i < functions.size(); i++) {
    if (functions[i]->row != i) {
      functions[i]->row = i;
      functions[i]->setPos(30, i * 30);
      emit model->positionChanged(model->createIndex(i, 0, static_cast<OperationsItem *>(functions[i])));
    }
  }
}

void OperationsItem::setCollapsed(bool c)
{
  collapsed = c;
  subItems.setVisible(!c);
}

SiteItem::SiteItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent, Qt::green), name(name_) {}

SiteItem::~SiteItem()
{
  for (ProgramItem *program : programs) {
    delete program;
  }
}

QVariant SiteItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

void SiteItem::reorder(OperationsModel const *model)
{
  for (int i = 0; (size_t)i < programs.size(); i++) {
    if (programs[i]->row != i) {
      programs[i]->row = i;
      programs[i]->setPos(30, i * 90);
      emit model->positionChanged(model->createIndex(i, 0, static_cast<OperationsItem *>(programs[i])));
    }
  }
}


