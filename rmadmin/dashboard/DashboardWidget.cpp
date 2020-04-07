#include <QDebug>
#include <QMenu>
#include <QMenuBar>
#include <QPushButton>
#include <QVBoxLayout>
#include "AtomicWidget.h"
#include "conf.h"
#include "confValue.h"
#include "dashboard/Dashboard.h"
#include "dashboard/DashboardCopyDialog.h"
#include "dashboard/DashboardSelector.h"
#include "dashboard/DashboardWidgetText.h"
#include "dashboard/DashboardWidgetChart.h"
#include "dashboard/tools.h"
#include "Resources.h"
#include "TimeRangeEdit.h"

#include "dashboard/DashboardWidget.h"

static bool const verbose(false);

DashboardWidget::DashboardWidget(
  Dashboard *dashboard_,
  DashboardWidgetForm *widgetForm_,
  QWidget *parent)
  : AtomicWidget(parent),
    dashboard(dashboard_),
    widgetForm(widgetForm_),
    widgetText(nullptr),
    widgetChart(nullptr),
    current(nullptr)
{
}

void DashboardWidget::setEnabled(bool enabled)
{
  if (current) current->setEnabled(enabled);
}

std::shared_ptr<conf::Value const> DashboardWidget::getValue() const
{
  if (current) return current->getValue();
  return nullptr;
}

bool DashboardWidget::setValue(
  std::string const &key, std::shared_ptr<conf::Value const> val)
{
  std::shared_ptr<conf::DashWidgetText const> confText =
    std::dynamic_pointer_cast<conf::DashWidgetText const>(val);

  AtomicWidget *newCurrent;
  QString newTitle;

  if (confText) {

    if (! widgetText) {
      if (verbose)
        qDebug() << "DashboardWidget: create a new text widget";

      if (widgetChart) {
        widgetChart->deleteLater();
        widgetChart = nullptr;
      }

      widgetText = new DashboardWidgetText(widgetForm, this);
      widgetText->setKey(key);
    }
    newCurrent = widgetText;

  } else {

    std::shared_ptr<conf::DashWidgetChart const> confChart =
      std::dynamic_pointer_cast<conf::DashWidgetChart const>(val);

    if (!confChart) {
      qCritical("confkey %s is not a DashboardWidget", key.c_str());
      return false;
    }

    if (! widgetChart) {
      if (verbose)
        qDebug() << "DashboardWidget: create a new chart widget for key"
                 << QString::fromStdString(key);

      if (widgetText) {
        widgetText->deleteLater();
        widgetText = nullptr;
      }

      widgetChart = new DashboardWidgetChart(widgetForm, this);
      widgetChart->setKey(key);

      if (dashboard) {
        connect(dashboard->timeRangeEdit, &TimeRangeEdit::valueChanged,
                widgetChart, &DashboardWidgetChart::timeRangeChanged);
        widgetChart->setTimeRange(dashboard->timeRangeEdit->range);
        connect(widgetChart, &DashboardWidgetChart::newTailTime,
                dashboard, &Dashboard::setTailTime);
      }
    }
    newCurrent = widgetChart;
    newTitle = confChart->title;
  }

  if (newCurrent != current) {
    current = newCurrent;
    relayoutWidget(current);
  }

  if (newTitle != title) {
    title = newTitle;
    emit titleChanged(title);
  }

  return current->setValue(key, val);
}
