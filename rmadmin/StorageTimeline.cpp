#include <QLabel>
#include <QScrollArea>
#include <QVBoxLayout>
#include "TimeLineView.h"
#include "StorageTimeline.h"

StorageTimeline::StorageTimeline(
  GraphModel *graphModel,
  QWidget *parent)
  : QWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;

  QScrollArea *scrollArea = new QScrollArea;
  scrollArea->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
  scrollArea->setVerticalScrollBarPolicy(Qt::ScrollBarAlwaysOn);
  scrollArea->setWidgetResizable(true);

  // If you think this widget and layouts are useless, you are not alone:
  QWidget *widget = new QWidget;
  QVBoxLayout *l = new QVBoxLayout;
  widget->setLayout(l);

  timeLineView = new TimeLineView(graphModel);
  l->addWidget(timeLineView);
  l->setSizeConstraint(QLayout::SetMinimumSize);

  scrollArea->setWidget(widget);  // Must be added last for some reason

  layout->addWidget(scrollArea);

  // TODO: Add the explain combo and time picker

  setLayout(layout);
}
