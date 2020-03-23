#include <QDebug>
#include <QPushButton>
#include <QSplitter>
#include <QVBoxLayout>
#include <QWidget>
#include "chart/TimeChart.h"
#include "chart/TimeChartEditWidget.h"
#include "chart/TimeLine.h"
#include "chart/TimeLineGroup.h"
#include "Resources.h"

#include "chart/TimeChartEditor.h"

TimeChartEditor::TimeChartEditor(
  QPushButton *submitButton,
  QPushButton *cancelButton,
  TimeLineGroup *timeLineGroup,
  QWidget *parent)
  : QWidget(parent)
{
  editWidget = new TimeChartEditWidget(submitButton, cancelButton);

  chart = new TimeChart(editWidget);
  connect(chart, &TimeChart::newTailTime,
          this, &TimeChartEditor::newTailTime);

  TimeLine *timeLine = new TimeLine(0., 600., TimeLine::TicksTop);
  timeLine->setMinimumHeight(30);
  timeLine->setMaximumHeight(50);

  if (!timeLineGroup)
    timeLineGroup = new TimeLineGroup(this);

  timeLineGroup->add(chart);
  timeLineGroup->add(timeLine);
  connect(this, &TimeChartEditor::timeRangeChanged,
          timeLineGroup, &TimeLineGroup::setTimeRange);

  QVBoxLayout *timeLinesLayout = new QVBoxLayout;
  timeLinesLayout->addWidget(chart);
  timeLinesLayout->addWidget(timeLine);
  timeLinesLayout->setSpacing(0);
  timeLines = new QWidget(this);
  timeLines->setLayout(timeLinesLayout);

  QSplitter *splitter = new QSplitter;
  splitter->addWidget(editWidget);
  splitter->addWidget(timeLines);

  // layout
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(splitter);
  setLayout(layout);
}
