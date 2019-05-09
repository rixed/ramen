#ifndef GRAPHARROW_H_190509
#define GRAPHARROW_H_190509
#include <QGraphicsItem>

class GraphArrow : public QGraphicsItem
{
  QGraphicsItem const *from;
  QGraphicsItem const *to;

public:
  // [from] and [to] must outlive the arrow joining them:
  GraphArrow(QGraphicsItem const *from, QGraphicsItem const *to, QGraphicsItem *parent = nullptr);
  ~GraphArrow();

  QRectF boundingRect() const override;
  void paint(QPainter *, const QStyleOptionGraphicsItem *, QWidget *) override;
};

#endif
