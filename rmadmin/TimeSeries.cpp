#include <limits>
#include <QVBoxLayout>
#include "qcustomplot.h"
#include "Chart.h"
#include "ChartDataSet.h"
#include "FunctionItem.h"
#include "TimeSeries.h"

TimeSeries::TimeSeries(Chart *chart_) :
  Graphic(chart_, ChartTypeTimeSeries),
  xMin(std::numeric_limits<double>::max()),
  xMax(std::numeric_limits<double>::min()),
  yMin(std::numeric_limits<double>::max()),
  yMax(std::numeric_limits<double>::min())
{
  QVBoxLayout *layout = new QVBoxLayout;
  plot = new QCustomPlot;
  layout->addWidget(plot);
  setLayout(layout);

  // create graph and assign data to it:
  plot->addGraph();
  addPoints();
  // give the axes some labels:
  plot->xAxis->setLabel(chart->dataSets[0]->name());
  plot->yAxis->setLabel(chart->dataSets[1]->name());
  plot->setInteractions(QCP::iRangeDrag | QCP::iRangeZoom | QCP::iSelectPlottables);

  connect(chart->dataSets[0], &ChartDataSet::valueAdded, this, &TimeSeries::appendValues);

  /* TODO:
   * In any case, all this must be editable by the user, so we can start with the
   * editor. But even for this we must first start with identifying the columns:
   * name, type, numeric-ness, time-ness, dimension, cardinality, proportion of
   * nulls.
   * Then ask what kind of chart is wanted (pie, histogram, heatmap, timechart)
   */
}

bool TimeSeries::addPoints(unsigned first)
{
  // Random content:
  size_t numTuples = chart->dataSets[0]->numRows();
  if (numTuples <= first) return false;
  numTuples -= first;

  QVector<double> x(numTuples), y(numTuples);
  for (unsigned i = 0; i < numTuples; ++i) {
    std::optional<double> v = chart->dataSets[0]->value(first + i)->toDouble();
    if (v) {
      x[i] = *v;
      if (*v > xMax) xMax = *v;
      if (*v < xMin) xMin = *v;
    }
    v = chart->dataSets[1]->value(first + i)->toDouble();
    if (v) {
      y[i] = *v;
      if (*v > yMax) yMax = *v;
      if (*v < yMin) yMin = *v;
    }
  }
  plot->graph(0)->addData(x, y);
  // set axes ranges, so we see all data:
  plot->xAxis->setRange(xMin, xMax);
  plot->yAxis->setRange(yMin, yMax); // Option to force 0 in the range

  return true;
}

void TimeSeries::update() const
{
  plot->replot();
}

void TimeSeries::appendValues()
{
  QSharedPointer<QCPGraphDataContainer> graphData = plot->graph(0)->data();
  if (addPoints(graphData->size())) update();
}
