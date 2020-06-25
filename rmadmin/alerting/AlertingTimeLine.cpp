#include <cstdlib>
#include <QDebug>
#include <QFormLayout>
#include "alerting/NotifTimeLine.h"
#include "alerting/tools.h"
#include "chart/TimeLine.h"
#include "chart/TimeLineGroup.h"
#include "conf.h"
#include "confValue.h"
#include "TimeRange.h"

#include "alerting/AlertingTimeLine.h"

AlertingTimeLine::AlertingTimeLine(QWidget *parent)
  : QWidget(parent)
{
  formLayout = new QFormLayout;
  formLayout->setSpacing(0);
  formLayout->setLabelAlignment(Qt::AlignLeft);
  // counter weird MacOS default:
  formLayout->setFieldGrowthPolicy(QFormLayout::AllNonFixedFieldsGrow);
  timeLineGroup = new TimeLineGroup(this);

  qreal const endOfTime = getTime();
  qreal const beginOfTime = endOfTime - 24*3600;
  TimeLine *timeLineTop =
    new TimeLine(beginOfTime, endOfTime, TimeLine::TicksBottom);
  TimeLine *timeLineBottom =
    new TimeLine(beginOfTime, endOfTime, TimeLine::TicksTop);
  /* Note: Order of insertion in the group has no influence over order of
   * representation in the QFormLayout: */
  timeLineGroup->add(timeLineTop);
  timeLineGroup->add(timeLineBottom);

  /* *Last* line will always be this timeLine. Lines will be added/removed
   * above to maintain a list of known archiving functions: */
  formLayout->addRow(QString(), timeLineTop);
  formLayout->addRow(QString(), timeLineBottom);

  /* TODO: Add time selector. */
  /* TODO: Add a search filter. */

  setLayout(formLayout);

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
  if (! timeLine) {
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
    formLayout->insertRow(row, incidentName, timeLine);
    timeLines.insert(incidentId, timeLine);
  }
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
