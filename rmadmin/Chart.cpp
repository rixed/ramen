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

  /* Selection of a default chart type: */
  if (dataSets.length() == 2 &&
      dataSets[0]->isNumeric() &&
      dataSets[1]->isNumeric()) {
    return new TimeSeries(this); // That was easy!
  }

  return new InvalidGraphic(this, "No possible chart with the selected columns");
}
