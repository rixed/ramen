#include <QVBoxLayout>
#include <QLabel>
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
