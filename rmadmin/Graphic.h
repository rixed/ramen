#ifndef GRAPHIC_190527
#define GRAPHIC_190527
#include <QWidget>
#include "Chart.h"

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
  /* Simple type of charts, giving the overall coordinate system and graphical
   * elements: */
  ChartType chartType;
  Chart *chart;

public:
  Graphic(Chart *chart_, ChartType chartType_) :
    QWidget(chart_), chartType(chartType_), chart(chart_) {}

  /* Reset the plot data from the chart iter function: */
  virtual void setData() = 0;

  /* Replot the graphics with the same data: */
  virtual void replot() const = 0;
};

class InvalidGraphic : public Graphic
{
  Q_OBJECT

  QLabel *label;
public:
  InvalidGraphic(Chart *, QString);
  void setData() {}
  void replot() const {}
};

#endif
