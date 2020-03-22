#include <QDebug>
#include <QLabel>
#include <QVBoxLayout>
#include "conf.h"
#include "confValue.h"
#include "dashboard/tools.h"
#include "dashboard/DashboardWidgetText.h"
#include "dashboard/DashboardWidgetChart.h"
#include "misc.h"
#include "TimeRange.h"
#include "TimeRangeEdit.h"

#include "dashboard/Dashboard.h"

//static bool const verbose(false);

/* The prefix must end with the dashboard name (before the "/widgets"
 * part). */
QString const dashboardNameOfKeyPrefix(std::string const prefix)
{
  std::string::size_type const len(prefix.length());
  assert(len > 1);
  assert(prefix[len - 1] != '/');
  if (startsWith(prefix, "dashboards/")) {
    return QString::fromStdString(prefix.substr(11));
  } else if (startsWith(prefix, "clients/") &&
             endsWith(prefix, "/scratchpad")) {
    return QString("scratchpad");
  } else {
    qFatal("Cannot make sense of key prefix %s", prefix.c_str());
  }
}

Dashboard::Dashboard(std::string const keyPrefix_, QWidget *parent)
  : QWidget(parent),
    keyPrefix(keyPrefix_),
    name(dashboardNameOfKeyPrefix(keyPrefix_))
{
  timeRangeEdit = new TimeRangeEdit;

  placeHolder = new QLabel(tr("This dashboard is empty"));

  vboxLayout = new QVBoxLayout;
  vboxLayout->addWidget(timeRangeEdit);
  vboxLayout->addWidget(placeHolder);

  setLayout(vboxLayout);

  connect(&kvs, &KVStore::valueCreated,
          this, &Dashboard::addValue);
  connect(&kvs, &KVStore::valueDeleted,
          this, &Dashboard::delValue);

  iterDashboardWidgets(keyPrefix,
    [this](std::string const &key, KValue const &kv, int idx) {
    addWidget(key, kv, idx);
  });
}

void Dashboard::addWidget(std::string const &key, KValue const &kv, int idx)
{
  DashboardWidget *widget;

  /* Create the new widget */
  std::shared_ptr<conf::DashboardWidgetText const> confText =
    std::dynamic_pointer_cast<conf::DashboardWidgetText const>(kv.val);
  if (confText) {
    widget = new DashboardWidgetText(key, this);
  } else {
    std::shared_ptr<conf::DashboardWidgetChart const> confChart =
      std::dynamic_pointer_cast<conf::DashboardWidgetChart const>(kv.val);
    if (confChart) {
      DashboardWidgetChart *widgetChart = new DashboardWidgetChart(key, this);
      connect(timeRangeEdit, &TimeRangeEdit::valueChanged,
              widgetChart, &DashboardWidgetChart::timeRangeChanged);
      connect(widgetChart, &DashboardWidgetChart::newTailTime,
              this, &Dashboard::setTailTime);
      widgetChart->setTimeRange(timeRangeEdit->range);
      widget = static_cast<DashboardWidget *>(widgetChart);
    } else {
      qCritical("confkey %s is not a DashboardWidget", key.c_str());
      return;
    }
  }
  assert(widget);

  /* Add the new widget at the proper position in the layout: */
  int layout_idx(0);
  for (std::list<WidgetRef>::iterator it = widgets.begin();
       it != widgets.end(); it++, layout_idx++) {
    if (it->idx == idx) {
      delete it->widget;
      it->widget = widget;
      goto added;
    } else if (it->idx > idx) {
      widgets.emplace(it, idx, widget);
      goto added;
    }
  }
  // fallback: add it at the end
  widgets.emplace_back(idx, widget);
added:

  vboxLayout->insertWidget(layout_idx, widget);

  placeHolder->setVisible(widgets.empty());
}

bool Dashboard::isMyKey(std::string const &key)
{
  return startsWith(key, keyPrefix);
}

void Dashboard::addValue(std::string const &key, KValue const &kv)
{
  if (! isMyKey(key)) return;

  std::optional<int> idx(widgetIndexOfKey(key));
  if (! idx) return;
  addWidget(key, kv, *idx);
}

void Dashboard::delWidget(int idx)
{
  for (std::list<WidgetRef>::iterator it = widgets.begin();
       it != widgets.end(); it++) {
    if (it->idx == idx) {
      widgets.erase(it);
      return;
    } else if (it->idx > idx) {
      qWarning() << "Unknown deleted widget index" << idx;
      return;
    }
  }

  placeHolder->setVisible(widgets.empty());
}

void Dashboard::delValue(std::string const &key, KValue const &)
{
  if (! isMyKey(key)) return;

  std::optional<int> idx(widgetIndexOfKey(key));
  if (! idx.has_value()) return;
  delWidget(*idx);
}

void Dashboard::setTailTime(double t)
{
  /* If the time range editor is relative (last ...) and this time is greater
   * than now, then advance the time range: */
  if (! timeRangeEdit->range.relative) return;
  double const now(getTime());
  if (t <= now) return;

  timeRangeEdit->offset(t - now);
}
