#include <limits>
#include <memory>

#include <QDebug>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QTimer>

#include "alerting/tools.h"
#include "alerting/AlertingStats.h"

static bool const verbose { false };

AlertingStats::AlertingStats(QWidget *parent)
  : QWidget(parent)
{
  lastNotificationWidget = new QLabel;
  oldestNotificationWidget = new QLabel;
  lastDeliveryAttemptWidget = new QLabel;
  nextScheduleWidget = new QLabel;
  nextSendWidget = new QLabel;
  numTeamsWidget = new QLabel;
  numIncidentsWidget = new QLabel;
  numFiringIncidentsWidget = new QLabel;
  numDialogsWidget = new QLabel;

  QHBoxLayout *layout = new QHBoxLayout;
  QFormLayout *leftCol = new QFormLayout;
  QFormLayout *rightCol = new QFormLayout;
  leftCol->addRow(tr("Number of Firing Incidents:"), numFiringIncidentsWidget);
  leftCol->addRow(tr("Number of Incidents:"), numIncidentsWidget);
  leftCol->addRow(tr("Number of Teams:"), numTeamsWidget);
  leftCol->addRow(tr("Number of Messaging contexts:"), numDialogsWidget);
  rightCol->addRow(tr("Last Notification:"), lastNotificationWidget);
  rightCol->addRow(tr("Oldest Notification:"), oldestNotificationWidget);
  rightCol->addRow(tr("Last Delivery Attemps:"), lastDeliveryAttemptWidget);
  rightCol->addRow(tr("Next Scheduled Run:"), nextScheduleWidget);
  rightCol->addRow(tr("Next Scheduled Delivery:"), nextSendWidget);
  layout->addLayout(leftCol);
  layout->addLayout(rightCol);
  setLayout(layout);

  timer = new QTimer(this);
  connect(timer, &QTimer::timeout,
          this, QOverload<>::of(&AlertingStats::updateStats));

  connect(kvs, &KVStore::keyChanged,
          this, &AlertingStats::onChange);

  timer->start(2000);
}

void AlertingStats::updateStats()
{
  if (verbose)
    qDebug() << "AlertingStats::updateStats: dirty=" << dirty;

  if (! dirty) return;

  int numFiringIncidents = 0;
  int numIncidents = 0;
  int numDialogs[conf::DeliveryStatus::NUM_STATUS] = { 0 };
  double lastNotification = 0.;
  double oldestNotification = std::numeric_limits<double>::max();
  double lastDeliveryAttempt = 0.;
  double nextSchedule = std::numeric_limits<double>::max();
  double nextSend = std::numeric_limits<double>::max();

  std::function<void(std::shared_ptr<conf::Notification const>)> updateNotifDates {
    [&lastNotification, &oldestNotification]
    (std::shared_ptr<conf::Notification const> notif)
    {
      lastNotification = std::max<double>(lastNotification, notif->sentTime);
      oldestNotification = std::min<double>(oldestNotification, notif->sentTime);
    }
  };

  /* Update all stats in one go: */
  int numTeams = 0;
  iterTeams([&numTeams](std::string const &) {
    numTeams ++;
  });

  iterIncidents([&numFiringIncidents, &numIncidents, &numDialogs,
                 &lastDeliveryAttempt, &nextSchedule, &nextSend,
                 &updateNotifDates](std::string const &incidentId) {
    numIncidents ++;

    std::shared_ptr<conf::Notification const> firstStartNotif {
      std::dynamic_pointer_cast<conf::Notification const>(
          getIncident(incidentId, "first_start")) };
    if (firstStartNotif) {
      updateNotifDates(firstStartNotif);
    }

    std::shared_ptr<conf::Notification const> lastChangeNotif {
      std::dynamic_pointer_cast<conf::Notification const>(
          getIncident(incidentId, "last_change")) };
    if (lastChangeNotif) {
      if (lastChangeNotif->firing) numFiringIncidents ++;
      updateNotifDates(lastChangeNotif);
    }

    iterDialogs(incidentId,
                [&numDialogs, &lastDeliveryAttempt, &nextSchedule, &nextSend,
                 &incidentId](std::string const &dialogId) {
      std::shared_ptr<conf::DeliveryStatus const> deliveryStatus {
        std::dynamic_pointer_cast<conf::DeliveryStatus const>(
            getDialog(incidentId, dialogId, "delivery_status")) };
      if (deliveryStatus) {
        assert(deliveryStatus->status < conf::DeliveryStatus::NUM_STATUS);
        numDialogs[deliveryStatus->status] ++;
      }

      std::optional<double> const lastDelivAttempt {
        getDialogDate(incidentId, dialogId, "last_attempt") };
      if (lastDelivAttempt)
        lastDeliveryAttempt =
          std::max<double>(lastDeliveryAttempt, *lastDelivAttempt);

      std::optional<double> const nextSched {
        getDialogDate(incidentId, dialogId, "next_scheduled") };
      if (nextSched)
        nextSchedule =
          std::min<double>(nextSchedule, *lastDelivAttempt);

      std::optional<double> const nextSnd {
        getDialogDate(incidentId, dialogId, "next_send") };
      if (nextSnd)
        nextSend =
          std::min<double>(nextSend, *nextSnd);
    });
  });

  numTeamsWidget->setText(QString::number(numTeams));

  numFiringIncidentsWidget->setText(QString::number(numFiringIncidents));
  numIncidentsWidget->setText(QString::number(numIncidents));
  QStringList numDialogsStr;
  if (numDialogs[conf::DeliveryStatus::StartToBeSent] > 0)
    numDialogsStr.append(
      QString::number(numDialogs[conf::DeliveryStatus::StartToBeSent]) +
      " alerts to be sent");
  if (numDialogs[conf::DeliveryStatus::StartToBeSentThenStopped] > 0)
    numDialogsStr.append(
      QString::number(numDialogs[conf::DeliveryStatus::StartToBeSentThenStopped]) +
      " stopped before alert delivery attempted");
  if (numDialogs[conf::DeliveryStatus::StartSent] > 0)
    numDialogsStr.append(
      QString::number(numDialogs[conf::DeliveryStatus::StartSent]) +
      " alerts sent");
  if (numDialogs[conf::DeliveryStatus::StartAcked] > 0)
    numDialogsStr.append(
      QString::number(numDialogs[conf::DeliveryStatus::StartAcked]) +
      " alerts acked");
  if (numDialogs[conf::DeliveryStatus::StopToBeSent] > 0)
    numDialogsStr.append(
      QString::number(numDialogs[conf::DeliveryStatus::StopToBeSent]) +
      " recovery to be sent");
  if (numDialogs[conf::DeliveryStatus::StopSent] > 0)
    numDialogsStr.append(
      QString::number(numDialogs[conf::DeliveryStatus::StopSent]) +
      " recovery sent");
  numDialogsWidget->setText(numDialogsStr.join(",\n"));
  lastNotificationWidget->setText(stringOfDate(lastNotification));
  oldestNotificationWidget->setText(stringOfDate(oldestNotification));
  lastDeliveryAttemptWidget->setText(stringOfDate(lastDeliveryAttempt));
  nextScheduleWidget->setText(stringOfDate(nextSchedule));
  nextSendWidget->setText(stringOfDate(nextSend));
}

bool AlertingStats::isMyKey(std::string const &k) const
{
  return
    startsWith(k, "alerting/teams/") ||
    startsWith(k, "alerting/incidents/");
}

void AlertingStats::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyLocked:
      case KeyUnlocked:
        break;
      case KeyCreated:
      case KeyChanged:
      case KeyDeleted:
        if (! isMyKey(change.key)) return;
        dirty = true;
        break;
    }
  }
}
