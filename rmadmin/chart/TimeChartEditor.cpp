#include <QDebug>
#include <QPushButton>
#include <QResizeEvent>
#include <QSizePolicy>
#include <QVBoxLayout>
#include <QWidget>
#include "chart/TimeChart.h"
#include "chart/TimeChartEditWidget.h"
#include "chart/TimeLine.h"
#include "chart/TimeLineGroup.h"
#include "Resources.h"

#include "chart/TimeChartEditor.h"

static int const minTimeLineHeight(25); // below that, hide it

TimeChartEditor::TimeChartEditor(
  QPushButton *submitButton,
  QPushButton *cancelButton,
  TimeLineGroup *timeLineGroup,
  QWidget *parent)
  : QWidget(parent)
{
  editWidget = new TimeChartEditWidget(submitButton, cancelButton);
  /* Make closing the window the same as cancel */

  chart = new TimeChart(editWidget, this);
  connect(chart, &TimeChart::newTailTime,
          this, &TimeChartEditor::newTailTime);

  timeLine = new TimeLine(0., 600., TimeLine::TicksTop);
  timeLine->setMinimumHeight(minTimeLineHeight);
  timeLine->setMaximumHeight(50);

  if (!timeLineGroup)
    timeLineGroup = new TimeLineGroup(this);

  timeLineGroup->add(chart);
  timeLineGroup->add(timeLine);
  connect(this, &TimeChartEditor::timeRangeChanged,
          timeLineGroup, &TimeLineGroup::setTimeRange);

  QVBoxLayout *timeLinesLayout = new QVBoxLayout;
  timeLinesLayout->addWidget(chart, 1);
  timeLinesLayout->addWidget(timeLine, 0);
  timeLinesLayout->setSpacing(0);
  timeLines = new QWidget(this);
  timeLines->setObjectName("timeLines");
  timeLines->setLayout(timeLinesLayout);
  timeLines->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);

  // layout
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(timeLines);
  setLayout(layout);

  setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
}

TimeChartEditor::~TimeChartEditor()
{
  // As we are not its parent we have to delete it explicitly
  editWidget->deleteLater();
}

void TimeChartEditor::resizeEvent(QResizeEvent *)
{
  int const minChartHeight(chart->minimumSize().height());

  if (timeLine->isVisible()) {
    if (chart->height() <= minChartHeight)
      timeLine->setVisible(false);
  } else {
    if (chart->height() > minChartHeight + minTimeLineHeight) {
      timeLine->setVisible(true);
      timeLine->resize(timeLine->width(), minTimeLineHeight);
      chart->resize(chart->width(), chart->height() - minTimeLineHeight);
    }
  }
}
