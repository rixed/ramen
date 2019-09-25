#ifndef TIMESERIES_190925
#define TIMESERIES_190925
#include "Graphic.h"

class QCustomPlot;

class TimeSeries : public Graphic
{
  Q_OBJECT

  QCustomPlot *plot;
  double xMin, xMax, yMin, yMax;
public:
  TimeSeries(Chart *);
  void update() const;
  bool addPoints(unsigned first = 0);

protected slots:
  virtual void appendValues();
};

#endif
