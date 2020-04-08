#include <QDebug>
#include <QLabel>
#include <QMargins>
#include <QScrollArea>
#include <QSplitter>
#include <QVBoxLayout>
#include "chart/TimeLineGroup.h"
#include "conf.h"
#include "confValue.h"
#include "dashboard/tools.h"
#include "dashboard/DashboardWidgetForm.h"
#include "misc.h"
#include "TimeRange.h"
#include "TimeRangeEdit.h"

#include "dashboard/Dashboard.h"

static bool const verbose(false);

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
  timeLineGroup = new TimeLineGroup(this);

  placeHolder = new QLabel(tr("This dashboard is empty"));
  placeHolder->setObjectName("placeHolder");
  splitter = new QSplitter(Qt::Vertical);

  QScrollArea *scrollArea = new QScrollArea(this);
  scrollArea->setVerticalScrollBarPolicy(Qt::ScrollBarAsNeeded);
  scrollArea->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
  scrollArea->setWidgetResizable(true);
  scrollArea->setWidget(splitter);

  QVBoxLayout *vboxLayout = new QVBoxLayout;
  vboxLayout->setMargin(0);
  vboxLayout->setContentsMargins(QMargins());
  vboxLayout->addWidget(placeHolder);
  vboxLayout->addWidget(scrollArea);
  vboxLayout->addWidget(timeRangeEdit);

  setLayout(vboxLayout);

  connect(kvs, &KVStore::keyChanged,
          this, &Dashboard::onChange);

  iterDashboardWidgets(keyPrefix,
    [this](std::string const &key, KValue const &, int idx) {
    addWidget(key, idx);
  });
}

void Dashboard::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
        addValue(change.key, change.kv);
        break;
      case KeyDeleted:
        delValue(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

void Dashboard::addWidget(std::string const &key, int idx)
{
  if (verbose)
    qDebug() << "Dashboard: addWidget" << QString::fromStdString(key)
             << "at index" << idx;

  DashboardWidgetForm *widgetForm(
    new DashboardWidgetForm(key, this, this));

  /* Add the new widget at the proper position in the layout: */
  int splitterIdx { 0 };
  for (std::list<WidgetRef>::iterator it = widgets.begin();
       it != widgets.end(); it++, splitterIdx++) {
    if (it->idx == idx) {
      if (verbose)
        qDebug() << "Dashboard: replacing widget" << idx;
      it->widget->deleteLater(); // also removes from the splitter
      it->widget = widgetForm;
      goto added;
    } else if (it->idx > idx) {
      widgets.emplace(it, idx, widgetForm);
      goto added;
    }
  }
  // fallback: add it at the end
  widgets.emplace_back(idx, widgetForm);
added:
  splitter->insertWidget(splitterIdx, widgetForm);

  placeHolder->setVisible(widgets.empty());
}

void Dashboard::resetArrows()
{
  // Reset up/down arrows in menus:
  size_t const numWidgets(widgets.size());
  size_t i(0);
  for (std::list<WidgetRef>::iterator it = widgets.begin();
       it != widgets.end(); i++, it++)
    it->widget->enableArrowsForPosition(i, numWidgets);
}

bool Dashboard::isMyKey(std::string const &key)
{
  return startsWith(key, keyPrefix);
}

void Dashboard::addValue(std::string const &key, KValue const &)
{
  if (! isMyKey(key)) return;

  std::optional<int> idx(widgetIndexOfKey(key));
  if (! idx) return;
  addWidget(key, *idx);
  resetArrows();
}

void Dashboard::delWidget(int idx)
{
  for (std::list<WidgetRef>::iterator it = widgets.begin();
       it != widgets.end();
       it++) {
    if (it->idx == idx) {
      if (verbose)
        qDebug() << "Dashboard: Removing widget" << idx;
      it->widget->deleteLater();
      widgets.erase(it);
      break;
    } else if (it->idx > idx) {
      qWarning() << "Unknown deleted widget index" << idx;
      break;
    }
  }

  resetArrows();
  placeHolder->setVisible(widgets.empty());
}

void Dashboard::delValue(std::string const &key, KValue const &)
{
  if (! isMyKey(key)) return;

  if (verbose)
    qDebug() << "Dashboard: removing widget" << QString::fromStdString(key);

  std::optional<int> idx(widgetIndexOfKey(key));
  if (! idx.has_value()) {
    qCritical() << "Dashboard::delValue: Cannot find index of widget"
                << QString::fromStdString(key);
    return;
  }

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
