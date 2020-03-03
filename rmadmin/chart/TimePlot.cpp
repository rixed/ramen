#include "TimePlot.h"

TimePlot::TimePlot(
  qreal beginOftime, qreal endOfTime,
  bool withCursor,
  bool doScroll,
  QWidget *parent)
  : AbstractTimeLine(beginOftime, endOfTime, withCursor, doScroll, parent)
{}

void TimePlot::paintEvent(QPaintEvent *event)
{
  (void)event;
}
