#include <limits>
#include <QVBoxLayout>
#include "qcustomplot.h"
#include "Chart.h"
#include "ChartDataSet.h"
#include "FunctionItem.h"
#include "Graphic.h"

InvalidGraphic::InvalidGraphic(Chart *chart_, QString errorMessage) :
  Graphic(chart_, ChartTypeInvalid)
{
  QVBoxLayout *layout = new QVBoxLayout;
  label = new QLabel(errorMessage);
  label->setAlignment(Qt::AlignCenter);
  layout->addWidget(label);
  setLayout(layout);
}

TimeSeries::TimeSeries(Chart *chart_) :
  Graphic(chart_, ChartTypeTimeSeries)
{
  QVBoxLayout *layout = new QVBoxLayout;
  plot = new QCustomPlot;
  layout->addWidget(plot);
  setLayout(layout);

  // Random content:
  size_t numTuples = chart->dataSets[0]->numRows();
  QVector<double> x(numTuples), y(numTuples);
  assert(numTuples > 0);
  double xMin = std::numeric_limits<double>::max(), xMax = std::numeric_limits<double>::min();
  double yMin = std::numeric_limits<double>::max(), yMax = std::numeric_limits<double>::min();
  for (unsigned i = 0; i < numTuples; ++i) {
    std::optional<double> v = chart->dataSets[0]->value(i)->toDouble();
    if (v) {
      x[i] = *v;
      if (*v > xMax) xMax = *v;
      if (*v < xMin) xMin = *v;
    }
    v = chart->dataSets[1]->value(i)->toDouble();
    if (v) {
      y[i] = *v;
      if (*v > yMax) yMax = *v;
      if (*v < yMin) yMin = *v;
    }
  }
  // create graph and assign data to it:
  plot->addGraph();
  plot->graph(0)->setData(x, y);
  // give the axes some labels:
  plot->xAxis->setLabel(chart->dataSets[0]->name());
  plot->yAxis->setLabel(chart->dataSets[1]->name());
  // set axes ranges, so we see all data:
  plot->xAxis->setRange(xMin, xMax);
  plot->yAxis->setRange(yMin, yMax); // Option to force 0 in the range
  plot->setInteractions(QCP::iRangeDrag | QCP::iRangeZoom | QCP::iSelectPlottables);

  /* TODO:
   * In any case, all this must be editable by the user, so we can start with the
   * editor. But even for this we must first start with identifying the columns:
   * name, type, numeric-ness, time-ness, dimension, cardinality, proportion of
   * nulls.
   * Then ask what kind of chart is wanted (pie, histogram, heatmap, timechart)
   */
}

void TimeSeries::update() const
{
  plot->replot();
}
