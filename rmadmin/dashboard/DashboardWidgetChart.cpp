#include <QVBoxLayout>
#include "chart/TimeChartEditor.h"

#include "dashboard/DashboardWidgetChart.h"

DashboardWidgetChart::DashboardWidgetChart(
  std::string const &key,
  QWidget *parent)
  : DashboardWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;
  chart = new TimeChartEditor(key);
  /* TODO: connect the dashboard time control with the corresponding slots
     of the chart. */
  layout->addWidget(chart);
  setLayout(layout);
}
