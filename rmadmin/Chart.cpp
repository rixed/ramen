#include <QDebug>
#include <QVBoxLayout>
#include "RamenType.h"
#include "RamenValue.h"
#include "PastData.h"
#include "TailModel.h"
#include "TimeRangeEdit.h"
#include "TimeSeries.h"

#include "Chart.h"

static bool const verbose = false;

Chart::Chart(std::shared_ptr<TailModel const> tailModel_,
             std::shared_ptr<PastData> pastData_,
             std::vector<int> columns_, QWidget *parent) :
  QWidget(parent),
  tailModel(tailModel_),
  pastData(pastData_),
  columns(columns_),
  graphic(nullptr)
{
  timeRangeEdit = new TimeRangeEdit;
  connect(timeRangeEdit, &TimeRangeEdit::valueChanged,
          this, &Chart::updateChart);

  layout = new QVBoxLayout;
  layout->addWidget(timeRangeEdit);
  setLayout(layout);

  updateGraphic();

  connect(tailModel.get(), &TailModel::rowsInserted,
          this, &Chart::updateChart);
}

void Chart::iterValues(std::function<void (std::vector<RamenValue const *> const)> cb) const
{
  if (columns.size() == 0) return;

  /* Start with past data.
   * If that's past data, request this range just to be sure to have it at
   * some point.
   * do not ask for any time after the oldest tail tuple though. */
  TimeRange range = timeRangeEdit->getRange();
  double since, until;
  range.range(&since, &until);
  double reqSince = since, reqUntil = until;
  /* FIXME: lock tailModel->tuples here and release after having read the
   * first tuples */
  if (tailModel->rowCount() > 0) {
    double const oldestTail = tailModel->tuples[0].first;
    if (reqUntil > oldestTail) reqUntil = oldestTail;
  }
  if (reqSince < reqUntil)
    pastData->request(reqSince, reqUntil);

  if (verbose)
    qDebug() << "Chart::iterValues since" << (uint64_t)since
             << "until" << (uint64_t)until
             << "with" << tailModel->rowCount();

  pastData->iterTuples(since, until,
    [&cb, this](std::shared_ptr<RamenValue const> tuple) {
      std::vector<RamenValue const *> v;
      v.reserve(columns.size());
      for (unsigned column : columns) {
        v.push_back(tuple->columnValue(column));
      }
      cb(v);
    });

  /* Then for tail data: */
  // TODO: lock the tailModel to prevent points being added while we iterate
  for (int row = 0; row < tailModel->rowCount(); row ++) {
    std::pair<double, std::unique_ptr<RamenValue const>> const &tuple =
      tailModel->tuples[row];
    if (tuple.first >= since && tuple.first < until) {
      std::vector<RamenValue const *> v;
      v.reserve(columns.size());
      for (unsigned column : columns)
        v.push_back(tuple.second->columnValue(column));
      cb(v);
    }
  }
}

void Chart::updateGraphic()
{
  if (graphic) {
    layout->removeWidget(graphic);
    graphic->setParent(nullptr);
    delete graphic;
  }

  graphic = defaultGraphic();
  layout->addWidget(graphic, 0, 0);

  graphic->update();
}

/* This is called whenever a new dataSource is added (or removed) */
Graphic *Chart::defaultGraphic()
{
  // TODO: If there is an event time, add it to the columns?

  if (columns.size() != 2)
    return new InvalidGraphic(this, tr("You need to select two columns"));

  if (! tailModel->isNumeric(columns[0]) ||
      ! tailModel->isNumeric(columns[1]))
    return new InvalidGraphic(this, tr("Columns must both be numeric"));

  if (tailModel->rowCount() <= 0)
    return new InvalidGraphic(this, tr("No values"));

  /* TODO: Selection of a default chart type: */
  return new TimeSeries(this); // That was easy!
}

void Chart::updateChart()
{
  if (verbose)
    qDebug() << "Chart::updateChart";

  graphic->setData();
}

QString const Chart::labelName(int idx) const
{
  return tailModel->type->structure->columnName(columns[idx]);
}
