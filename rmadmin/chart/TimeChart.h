#ifndef TIMECHART_H_200203
#define TIMECHART_H_200203
#include "AbstractTimeLine.h"

class TimeChart : public AbstractTimeLine
{
  Q_OBJECT

public:
  TimeChart(QWidget *parent = nullptr);

protected slots:
  void paintEvent(QPaintEvent *event) override;
};

#endif
