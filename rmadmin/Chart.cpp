#include <iostream>
#include <QVBoxLayout>
#include "ChartDataSet.h"
#include "TimeSeries.h"
#include "TimeIntervalEdit.h"
#include "Chart.h"

static bool const verbose = true;

Chart::Chart(QWidget *parent) :
  QWidget(parent), graphic(nullptr)
{
  timeIntervalEdit = new TimeIntervalEdit;
  connect(timeIntervalEdit, &TimeIntervalEdit::valueChanged,
          this, &Chart::updateChart);

  layout = new QVBoxLayout;
  layout->addWidget(timeIntervalEdit);
  setLayout(layout);

  updateGraphic();
}

Chart::~Chart()
{
  reset();
}

void Chart::addData(ChartDataSet *ds)
{
  dataSets.append(ds);
}

void Chart::reset()
{
  /* TODO: Ideally this chart keeps memory of which dataset _names_ were
   * associated to which dimension. */
  while (! dataSets.empty()) delete dataSets.takeFirst();
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
  // TODO: If there is an event time, add it to the dataset?

  if (dataSets.length() != 2)
    return new InvalidGraphic(this, tr("You need to select two columns"));

  if (! dataSets[0]->isNumeric() ||
      ! dataSets[1]->isNumeric())
    return new InvalidGraphic(this, tr("Columns must both be numeric"));

  if (dataSets[0]->numRows() <= 0)
    return new InvalidGraphic(this, tr("No values"));

  /* TODO: Selection of a default chart type: */
  return new TimeSeries(this); // That was easy!
}

void Chart::updateChart()
{
  if (verbose)
    std::cout << "Chart::updateChart" << std::endl;
}
