#include <QVBoxLayout>
#include "qcustomplot.h"
#include "Chart.h"
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
  QVector<double> x(101), y(101); // initialize with entries 0..100
  for (int i=0; i<101; ++i) {
    x[i] = i/50.0 - 1; // x goes from -1 to 1
    y[i] = x[i]*x[i]; // let's plot a quadratic function
  }
  // create graph and assign data to it:
  plot->addGraph();
  plot->graph(0)->setData(x, y);
  // give the axes some labels:
  plot->xAxis->setLabel("x");
  plot->yAxis->setLabel("y");
  // set axes ranges, so we see all data:
  plot->xAxis->setRange(-1, 1);
  plot->yAxis->setRange(0, 1);
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
