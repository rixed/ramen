#ifndef GRAPHANCHOR_H_190509
#define GRAPHANCHOR_H_190509
#include <QGraphicsItem>

class GraphAnchor : public QGraphicsItem
{
public:
  GraphAnchor(QGraphicsItem *parent = nullptr);
  ~GraphAnchor();
  QRectF boundingRect() const override;
  void paint(QPainter *, const QStyleOptionGraphicsItem *, QWidget *) override;
};

#endif
