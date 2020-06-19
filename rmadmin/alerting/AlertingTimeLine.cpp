#include <cstdlib>
#include <QDebug>
#include <QGridLayout>
#include <QLabel>
#include "alerting/NotifTimeLine.h"
#include "alerting/tools.h"
#include "chart/TimeLine.h"
#include "chart/TimeLineGroup.h"
#include "conf.h"
#include "confValue.h"
#include "TimeRange.h"

#include "alerting/AlertingTimeLine.h"

static bool const verbose { false };

AlertingTimeLine::AlertingTimeLine(QWidget *parent)
  : QWidget(parent)
{
  gridLayout = new QGridLayout;
  /* If the QGridLayout is not the top-level layout (i.e. does not manage all
   * of the widget's area and children), you must add it to its parent layout
   * when you create it, but before you do anything with it. */
  setLayout(gridLayout);

  gridLayout->setSpacing(0);
  gridLayout->setContentsMargins(0, 0, 0, 0);

  /* Only the middle column with the timeline can grow: */
  gridLayout->setColumnStretch(0, 0);
  gridLayout->setColumnStretch(1, 1);
  gridLayout->setColumnStretch(2, 0);
  gridLayout->setColumnMinimumWidth(0, 50);
  gridLayout->setColumnMinimumWidth(1, 50);
  gridLayout->setColumnMinimumWidth(2, 16);
  timeLineGroup = new TimeLineGroup(this);

  qreal const endOfTime = getTime();
  qreal const beginOfTime = endOfTime - 24*3600;
  TimeLine *timeLineTop =
    new TimeLine(beginOfTime, endOfTime, TimeLine::TicksBottom);
  TimeLine *timeLineBottom =
    new TimeLine(beginOfTime, endOfTime, TimeLine::TicksTop);
  /* Note: Order of insertion in the group has no influence over order of
   * representation in the layout: */
  timeLineGroup->add(timeLineTop);
  timeLineGroup->add(timeLineBottom);

  /* *Last* line will always be this timeLine. Lines will be added/removed
   * above to maintain a list of known archiving functions: */
  gridLayout->addWidget(timeLineTop, 0, 1);
  gridLayout->addWidget(timeLineBottom, 1, 1);

  /* TODO: Add time selector. */
  /* TODO: Add a search filter. */

  iterIncidents([this](std::string const &incidentId) {
    iterLogs(incidentId, [this, &incidentId]
             (double time, std::shared_ptr<conf::IncidentLog const> log) {
      addLog(incidentId, time, log);
    });
  });

  connect(kvs, &KVStore::keyChanged,
          this, &AlertingTimeLine::onKeyChange);
}

void AlertingTimeLine::updateVisibility()
{
}

/* Just record it and update the visibility flag of existing time lines */
void AlertingTimeLine::setTimeRange(TimeRange const &range)
{
  timeRange = range;
  updateVisibility();
}

void AlertingTimeLine::addLog(
  std::string const &incidentId,
  double const &time,
  std::shared_ptr<conf::IncidentLog const> log)
{
  (void)time; (void)log;
  NotifTimeLine *timeLine { timeLines.value(incidentId) };
  if (timeLine) return;

  std::shared_ptr<conf::Notification const> const firstStart {
    getIncidentNotif(incidentId, "first_start") };
  if (! firstStart) {
    qWarning() << "Cannot find first_start notif for incident"
               << QString::fromStdString(incidentId);
    return;
  }

  std::shared_ptr<VString const> const assignedTeam {
    getAssignedTeam(incidentId) };
  if (! assignedTeam) {
    qWarning() << "Cannot find IncidentId" << QString::fromStdString(incidentId)
               << "(assigned team (absent or invalid type)";
    return;
  }

  QString const incidentName {
    assignedTeam->v + QString(": ") + firstStart->name };

  timeLine = new NotifTimeLine(incidentId, 0., 0., true, true, this);
  timeLineGroup->add(timeLine);
  int const row = 1; // TODO: order notif names alphabetically?
  insertRow(row, incidentName, timeLine);
  timeLines.insert(incidentId, timeLine);
}

void AlertingTimeLine::insertRow(
  int row, QString const &name, NotifTimeLine *timeLine)
{
  // Move bottom rows one position down:
  for (int r = gridLayout->rowCount(); r > row; r--) {
    for (int c = 0; c < 3; c++) {
      QLayoutItem *item = gridLayout->itemAtPosition(r - 1, c);
      if (item) {
        gridLayout->removeItem(item);
        gridLayout->addItem(item, r, c);
      }
    }
  }
  gridLayout->addWidget(new QLabel(name), row, 0);
  gridLayout->addWidget(timeLine, row, 1);
  /* Check if this is still waiting for an ack.
   * The GUI is the simplest way to ack an alert. */
  
  gridLayout->addWidget(new QLabel(tr("TODO")), row, 2);
}

void AlertingTimeLine::addLogKey(std::string const &key, KValue const &kv)
{
  std::string incidentId;
  double time;
  if (! parseLogKey(key, &incidentId, &time)) return;

  std::shared_ptr<conf::IncidentLog const> log {
    std::dynamic_pointer_cast<conf::IncidentLog const>(kv.val) };
  if (! log) return;  // Not all keys are log events

  addLog(incidentId, time, log);
}

void AlertingTimeLine::onKeyChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    if (change.op != KeyCreated) continue;
    addLogKey(change.key, change.kv);
  }
}
