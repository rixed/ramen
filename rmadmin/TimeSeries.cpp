#include <limits>
#include <cassert>
#include <optional>
#include <QVBoxLayout>
#include <QVector>
#include <QCheckBox>
#include "qcustomplot.h"
#include "Chart.h"
#include "FunctionItem.h"
#include "TimeSeries.h"

static bool const verbose = true;

QSharedPointer<QCPAxisTickerDateTime> TimeSeries::dateTicker(
  new QCPAxisTickerDateTime());

TimeSeries::TimeSeries(Chart *chart_) :
  Graphic(chart_, ChartTypeTimeSeries),
  xMin(std::numeric_limits<double>::max()),
  xMax(std::numeric_limits<double>::min()),
  yMin(std::numeric_limits<double>::max()),
  yMax(std::numeric_limits<double>::min()),
  xDataset(0),
  timeUnit(1.),
  y1Dataset(1),
  y2Dataset(-1),
  factor1(-1),
  factor2(-1)
{
  plot = new QCustomPlot;
  forceZeroCheckBox = new QCheckBox(tr("Force Zero"));
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(forceZeroCheckBox);
  layout->addWidget(plot);
  setLayout(layout);

  // Create graph and assign data to it:
  plot->addGraph();
  setData();
  // Give the axes some labels:
  // TODO: if there are several Y Axis, use a legend instead:
  // Or another graphic kind would have been chosen:
  assert(chart->numColumns() >= 2);
  plot->xAxis->setLabel(chart->labelName(xDataset));
  plot->yAxis->setLabel(chart->labelName(y1Dataset));

  // Also format the X tick marks as dates:
  plot->xAxis->setTicker(dateTicker);

  plot->setInteractions(QCP::iRangeDrag | QCP::iRangeZoom | QCP::iSelectPlottables);

  connect(forceZeroCheckBox, &QCheckBox::stateChanged,
          this, &TimeSeries::reformat);

  /* TODO:
   * In any case, all this must be editable by the user, so we can start with the
   * editor. But even for this we must first start with identifying the columns:
   * name, type, numeric-ness, time-ness, dimension, cardinality, proportion of
   * nulls.
   * Then ask what kind of chart is wanted (pie, histogram, heatmap, timechart)
   */
}

void TimeSeries::setData()
{
  QVector<QCPGraphData> vs;
  vs.reserve(100);

  xMin = yMin = std::numeric_limits<double>::max();
  xMax = yMax = std::numeric_limits<double>::min();

  chart->iterValues([&vs, this](std::vector<RamenValue const *> const values) {
    std::optional<double> xv =
      values[xDataset] != nullptr ? values[xDataset]->toDouble() : std::nullopt;

    if (! xv) return;

    double const t = *xv * timeUnit;
    if (t > xMax) xMax = t;
    if (t < xMin) xMin = t;
    if (verbose)
      qDebug() << "TimeSeries::setData: adding" << (uint64_t)t;

    std::optional<double> yv =
      values[y1Dataset] != nullptr ? values[y1Dataset]->toDouble() : std::nullopt;
    if (yv) {
      if (*yv > yMax) yMax = *yv;
      if (*yv < yMin) yMin = *yv;
    }
    // Note: qcustomplot uses NaN for a gap:
    vs.append(QCPGraphData(t, yv ? *yv : qQNaN()));
  });

  if (vs.size() == 0) return;

  plot->graph(0)->data()->set(vs);

  reformat();

  replot();
}

void TimeSeries::reformat()
{
  bool const forceZero =
    forceZeroCheckBox->checkState() == Qt::Checked;

  // Set axes ranges, so all data can be seen:
  plot->xAxis->setRange(xMin, xMax);
  plot->yAxis->setRange(
    forceZero ? std::min(0., yMin) : yMin,
    forceZero ? std::max(0., yMax) : yMax);
}

void TimeSeries::replot() const
{
  plot->replot();
}
