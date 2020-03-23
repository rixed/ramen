#include <QHBoxLayout>
#include "chart/TimeChartEditor.h"
#include "chart/TimeChartEditWidget.h"

#include "dashboard/DashboardWidgetChart.h"

DashboardWidgetChart::DashboardWidgetChart(
  std::string const &key,
  TimeLineGroup *timeLineGroup,
  QWidget *parent)
  : DashboardWidget(key, parent)
{
  chart = new TimeChartEditor(submitButton, cancelButton, timeLineGroup);
  addWidget(chart->editWidget, true);
  chart->editWidget->setKey(key);
  chart->editWidget->setVisible(false);

  connect(this, &DashboardWidgetChart::timeRangeChanged,
          chart, &TimeChartEditor::timeRangeChanged);
  connect(chart, &TimeChartEditor::newTailTime,
          this, &DashboardWidgetChart::newTailTime);

  /* Open/close the editor when the AtomicForm is enabled/disabled: */
  connect(this, &DashboardWidgetChart::changeEnabled,
          this, [this](bool enabled) {
    chart->editWidget->setVisible(enabled);
  });

  setCentralWidget(chart);
}

void DashboardWidgetChart::setTimeRange(TimeRange const &range)
{
  emit timeRangeChanged(range);
}

AtomicWidget *DashboardWidgetChart::atomicWidget() const
{
  return chart->editWidget;
}
