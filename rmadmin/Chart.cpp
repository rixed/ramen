#include <iostream>
#include <QVBoxLayout>
#include "TailModel.h"
#include "TimeSeries.h"
#include "TimeIntervalEdit.h"
#include "RamenType.h"
#include "RamenValue.h"
#include "Chart.h"

static bool const verbose = true;

Chart::Chart(std::shared_ptr<TailModel const> tailModel_,
             std::vector<int> columns_, QWidget *parent) :
  QWidget(parent), tailModel(tailModel_), columns(columns_),
  graphic(nullptr)
{
  timeIntervalEdit = new TimeIntervalEdit;
  connect(timeIntervalEdit, &TimeIntervalEdit::valueChanged,
          this, &Chart::updateChart);

  layout = new QVBoxLayout;
  layout->addWidget(timeIntervalEdit);
  setLayout(layout);

  updateGraphic();

  connect(tailModel.get(), &TailModel::rowsInserted,
          this, &Chart::updateChart);
}

void Chart::iterValues(std::function<void (std::vector<RamenValue const *> const)> cb) const
{
  if (columns.size() == 0) return;

  /* Start with past data: */
  // TODO

  /* Then for tail data: */
  // TODO: lock the tailModel to prevent points being added while we iterate
  int tailRowCount = tailModel->rowCount();
  for (int row = 0; row < tailRowCount; row ++) {
    std::vector<RamenValue const *> v;
    v.reserve(columns.size());
    for (unsigned column : columns) {
      if (row < tailModel->rowCount())
        v.push_back(tailModel->tuples[row]->columnValue(column));
      else
        v.push_back(nullptr);
    }
    cb(v);
  }
}

void Chart::updateGraphic()
{
  if (graphic) delete graphic;

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
    std::cout << "Chart::updateChart" << std::endl;

  graphic->setData();
}

QString const Chart::labelName(int idx) const
{
  return tailModel->type->structure->columnName(columns[idx]);
}
