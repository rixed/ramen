#include <QDebug>
#include <QSplitter>
#include <QVBoxLayout>
#include <QWidget>
#include "chart/TimeChart.h"
#include "chart/TimeChartEditForm.h"
#include "chart/TimeLine.h"
#include "chart/TimeLineGroup.h"

#include "chart/TimeChartEditor.h"

TimeChartEditor::TimeChartEditor(
  std::string const &key, QWidget *parent)
  : QWidget(parent)
{
  editForm = new TimeChartEditForm(key);

  chart = new TimeChart(editForm->editWidget);
  connect(chart, &TimeChart::newTailTime,
          this, &TimeChartEditor::newTailTime);

  TimeLine *timeLine = new TimeLine(0., 600., TimeLine::TicksTop);
  timeLine->setMinimumHeight(30);
  timeLine->setMaximumHeight(50);

  timeLineGroup = new TimeLineGroup(this);
  timeLineGroup->add(chart);
  timeLineGroup->add(timeLine);
  connect(this, &TimeChartEditor::timeRangeChanged,
          timeLineGroup, &TimeLineGroup::setTimeRange);

  QVBoxLayout *timeLinesLayout = new QVBoxLayout;
  timeLinesLayout->addWidget(chart);
  timeLinesLayout->addWidget(timeLine);
  timeLinesLayout->setSpacing(0);
  QWidget *timeLines = new QWidget;
  timeLines->setLayout(timeLinesLayout);

  QSplitter *splitter = new QSplitter;
  splitter->addWidget(editForm);
  splitter->addWidget(timeLines);

  // layout
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(splitter);
  setLayout(layout);
}
