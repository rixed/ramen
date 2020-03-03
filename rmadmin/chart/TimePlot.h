#ifndef TIMEPLOT_H_200203
#define TIMEPLOT_H_200203
/* A TimePlot is the widget that displays zero, one or several PlottedField
 * objects. */
#include "AbstractTimeLine.h"

class TimePlot : public AbstractTimeLine
{
  Q_OBJECT

public:
  TimePlot(
    qreal beginOfTime, qreal endOfTime,
    bool withCursor = true,
    bool doScroll = true,
    QWidget *parent = nullptr);

protected:
  void paintEvent(QPaintEvent *event) override;

private:
};

#endif
