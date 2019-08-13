#include <QGridLayout>
#include "ChartDataSet.h"
#include "Graphic.h"
#include "Chart.h"

Chart::Chart(QWidget *parent) :
  QWidget(parent), graphic(nullptr)
{
  layout = new QGridLayout;
  // TODO: add controls for the graph such as field selection, etc.
  setLayout(layout);
}

Chart::~Chart()
{
  for (auto *ds : dataSets) delete ds;
}

void Chart::addData(ChartDataSet *ds)
{
  dataSets.append(ds);
}

void Chart::update()
{
  if (! graphic) {
    graphic = defaultGraphic();
    layout->addWidget(graphic, 0, 0);
  }
  graphic->update();
}

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
