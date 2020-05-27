#ifndef ALERTINGTIMELINE_H_200527
#define ALERTINGTIMELINE_H_200527
#include <memory>
#include <QMap>
#include <QWidget>
#include "TimeRange.h"

struct ConfChange;
struct KValue;
class NotifTimeLine;
class QFormLayout;
class QLineEdit;
class TimeLineGroup;
namespace conf {
  struct IncidentLog;
};

class AlertingTimeLine : public QWidget
{
  Q_OBJECT

  QFormLayout *formLayout;
  TimeLineGroup *timeLineGroup;

  // NotifTimeLine are owned by this AlertingTimeLine:
  QMap<std::string, NotifTimeLine *> timeLines;

  /* Filters: */
  // If set, display only incidents with some logs in this range:
  TimeRange timeRange;
  // Display only incidents started with that notif name:
  QLineEdit *notifFilter;

  void updateVisibility();

  void addLog(
    std::string const &incidentId,
    double const &time,
    std::shared_ptr<conf::IncidentLog const> log);

  void addLogKey(
    std::string const &incidentId,
    KValue const &kv);

public:
  AlertingTimeLine(QWidget *parent = nullptr);

protected slots:
  void onKeyChange(QList<ConfChange> const &changes);
public slots:
  void setTimeRange(TimeRange const &);

};

#endif
