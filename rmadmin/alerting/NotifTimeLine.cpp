#include <vector>
#include <utility>
#include <QDebug>
#include <QPainter>
#include <QPaintEvent>
#include <QRect>
#include "confValue.h"
#include "misc.h"

#include "alerting/NotifTimeLine.h"
#include "alerting/tools.h"

NotifTimeLine::NotifTimeLine(
  std::string const incidentId_,
  qreal beginOftime, qreal endOfTime,
  bool withCursor,
  bool doScroll,
  QWidget *parent)
  : AbstractTimeLine(beginOftime, endOfTime, withCursor, doScroll, parent),
    incidentId(incidentId_)
{
}

void NotifTimeLine::paintEvent(QPaintEvent *event)
{
  /* In chronological order: */
  std::vector<std::pair<double, std::shared_ptr<conf::IncidentLog const>>> logs;
  iterLogs(incidentId,
           [&logs](double time, std::shared_ptr<conf::IncidentLog const> log) {
    logs.emplace_back(time, log);
  });
  struct {
    bool operator()(
      std::pair<double, std::shared_ptr<conf::IncidentLog const>> const &a,
      std::pair<double, std::shared_ptr<conf::IncidentLog const>> const &b) const
    {
      return a.first < b.first;
    }
  } chronologicalOrder;
  std::sort(logs.begin(), logs.end(), chronologicalOrder);

  QPainter painter(this);

  /* Paint the background: */
  std::optional<qreal> prevX { std::nullopt };
  for (std::pair<double, std::shared_ptr<conf::IncidentLog const>> const &l : logs) {
    double const time { l.first };
    std::shared_ptr<conf::IncidentLog const> const log { l.second };

    int const x { toPixel(time) };
    if (prevX) {
      painter.setPen(Qt::NoPen);

      switch (log->tickKind) {
        case conf::IncidentLog::TickStop:
          // pass
        case conf::IncidentLog::TickCancel:
          painter.setBrush(Qt::NoBrush);
          break;
        case conf::IncidentLog::TickDup:
          // pass
        case conf::IncidentLog::TickStart:
          painter.setBrush(Qt::white);
          break;
        case conf::IncidentLog::TickOutcry:
          painter.setBrush(Qt::red);
          break;
        case conf::IncidentLog::TickInhibited:
          // pass
        case conf::IncidentLog::TickAck:
          painter.setBrush(Qt::yellow);
          break;
      }

      painter.drawRect(*prevX, 0, x - *prevX, height());
    }

    prevX = x;
  }

  /* Paint the log ticks: */
  for (std::pair<double, std::shared_ptr<conf::IncidentLog const>> const &l : logs) {
    double const time { l.first };
    std::shared_ptr<conf::IncidentLog const> const log { l.second };

    int const x { toPixel(time) };
    log->paintTick(&painter, width(), x, 0, height());
  }

  /* Paint the cursor over: */
  AbstractTimeLine::paintEvent(event);
}
