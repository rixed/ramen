#ifndef HEATLINE_H_191204
#define HEATLINE_H_191204
/* A HeatLine is an AbstractTimeLine that displays colored blocks. */
#include <QColor>
#include <QPair>
#include <QList>
#include "chart/AbstractTimeLine.h"

class HeatLine : public AbstractTimeLine
{
  Q_OBJECT

public:
  // Created empty:
  HeatLine(
    qreal beginOfTime, qreal endOfTime,
    bool withCursor = true,
    bool doScroll = true,
    QWidget *parent = nullptr);

  void add(qreal, qreal, QColor const &);
  void reset() { blocks.clear(); }

protected:
  void paintEvent(QPaintEvent *event) override;

private:
  // Ordered by start, no overlap
  struct Block {
    qreal start, stop;
    QColor color;
  };
  QList<Block> blocks;
};

#endif
