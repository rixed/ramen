#ifndef ALERTINGSTATS_H_200525
#define ALERTINGSTATS_H_200525
#include <QWidget>

#include "conf.h"
#include "confValue.h"

class QLabel;
class QTimer;

class AlertingStats : public QWidget
{
  Q_OBJECT

  QLabel *numFiringIncidentsWidget;
  QLabel *numIncidentsWidget;
  QLabel *numTeamsWidget;
  QLabel *numDialogsWidget;
  QLabel *lastNotificationWidget;
  QLabel *oldestNotificationWidget;
  QLabel *lastDeliveryAttemptWidget;
  QLabel *nextScheduleWidget;
  QLabel *nextSendWidget;

  // Start dirty because of the missed sync:
  bool dirty = true;
  QTimer *timer;

  bool isMyKey(std::string const &) const;
  void updateStats();

public:
  AlertingStats(QWidget *parent = nullptr);

protected slots:
  void onChange(QList<ConfChange> const &);

};

#endif
