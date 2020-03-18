#include <QVBoxLayout>
#include "chart/TimeChartEditor.h"

#include "dashboard/DashboardWidgetChart.h"

DashboardWidgetChart::DashboardWidgetChart(
  std::string const &key,
  QWidget *parent)
  : DashboardWidget(parent)
{
  chart = new TimeChartEditor(key);

  connect(this, &DashboardWidgetChart::timeRangeChanged,
          chart, &TimeChartEditor::timeRangeChanged);
  connect(chart, &TimeChartEditor::newTailTime,
          this, &DashboardWidgetChart::newTailTime);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(chart);
  setLayout(layout);
}

void DashboardWidgetChart::setTimeRange(TimeRange const &range)
{
  emit timeRangeChanged(range);
}
