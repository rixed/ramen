#include "TimePlotAxis.h"

TimePlotAxis::TimePlotAxis(
  Side side_, bool forceZero_, Scale scale_)
  : side(side_),
    forceZero(forceZero_),
    scale(scale_),
    labelFormat(Decimal)
{}

void TimePlotAxis::paint(QWidget *widget) const
{
  (void)widget;
}
