#include <QHBoxLayout>
#include "chart/TimeChartEditor.h"
#include "chart/TimeChartEditWidget.h"
#include "dashboard/Dashboard.h"
#include "dashboard/DashboardWidgetForm.h"

#include "dashboard/DashboardWidgetChart.h"

DashboardWidgetChart::DashboardWidgetChart(
  DashboardWidgetForm *widgetForm,
  QWidget *parent)
  : AtomicWidget(parent)
{
  chart =
    new TimeChartEditor(
      widgetForm ? widgetForm->submitButton : nullptr,
      widgetForm ? widgetForm->cancelButton : nullptr,
      widgetForm ? widgetForm->dashboard->timeLineGroup : nullptr);

  connect(this, &DashboardWidgetChart::timeRangeChanged,
          chart, &TimeChartEditor::timeRangeChanged);
  connect(chart, &TimeChartEditor::newTailTime,
          this, &DashboardWidgetChart::newTailTime);

  /* Open/close the editor when the AtomicForm is enabled/disabled: */
  if (widgetForm) {
    chart->editWidget->setVisible(false);
    connect(widgetForm, &DashboardWidgetForm::changeEnabled,
            this, [this](bool enabled) {
      chart->editWidget->setVisible(enabled);
    });
  }

  relayoutWidget(chart);
}

void DashboardWidgetChart::setTimeRange(TimeRange const &range)
{
  emit timeRangeChanged(range);
}

void DashboardWidgetChart::setEnabled(bool enabled)
{
  chart->editWidget->setEnabled(enabled);
}

std::shared_ptr<conf::Value const> DashboardWidgetChart::getValue() const
{
  return chart->editWidget->getValue();
}

bool DashboardWidgetChart::setValue(
  std::string const &key, std::shared_ptr<conf::Value const> val)
{
  return chart->editWidget->setValue(key, val);
}
