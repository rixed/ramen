#ifndef GRAPHIC_190527
#define GRAPHIC_190527
#include "Chart.h"

class QCustomPlot;
class QLabel;

enum ChartType {
  ChartTypeInvalid,
  ChartTypeScatterPlot,
  ChartTypeScatterPlotMatrix,
  ChartTypePieChart,
  ChartTypeTimeSeries,
  ChartTypeTimeSeriesRows,
  ChartTypeHistogram,
  ChartTypeHeatMap,
  ChartTypeVennDiagram
};

class Graphic : public QWidget
{
  Q_OBJECT

protected:
  /* Simple type of charts, giving the overall coordinate system and grphical
   * elements: */
  ChartType chartType;
  Chart *chart;

public:
  Graphic(Chart *chart_, ChartType chartType_) :
    QWidget(chart_), chartType(chartType_), chart(chart_) {}
  virtual void update() const = 0;
};

class InvalidGraphic : public Graphic
{
  Q_OBJECT

  QLabel *label;
public:
  InvalidGraphic(Chart *, QString);
  void update() const {}
};

class TimeSeries : public Graphic
{
  Q_OBJECT

  QCustomPlot *plot;
public:
  TimeSeries(Chart *);
  void update() const;
};

#endif
