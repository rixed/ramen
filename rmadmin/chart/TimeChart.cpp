#include "TimeChart.h"

static double const defaultBeginOftime = 0.;
static double const defaultEndOfTime = 600.;

TimeChart::TimeChart(QWidget *parent)
  : AbstractTimeLine(defaultBeginOftime, defaultEndOfTime, true, true, parent)
{}

void TimeChart::paintEvent(QPaintEvent *event)
{
  (void)event;
}
