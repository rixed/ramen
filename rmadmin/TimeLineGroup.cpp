#include <QDebug>
#include <limits>
#include "AbstractTimeLine.h"
#include "misc.h"
#include "TimeLine.h"
#include "TimeLineGroup.h"

TimeLineGroup::TimeLineGroup(QObject *parent)
  : QObject(parent),
    minBeginOfTime(std::numeric_limits<qreal>::max()),
    maxEndOfTime(std::numeric_limits<qreal>::min()),
    largestViewPort(QPair<qreal, qreal>(
      std::numeric_limits<qreal>::max(),
      std::numeric_limits<qreal>::min()))
{
  items.reserve(20);
}

void TimeLineGroup::add(AbstractTimeLine *w)
{
  /* Make all time ranges the same: */
  if (w->beginOfTime() < minBeginOfTime) {
    minBeginOfTime = w->beginOfTime();
    qDebug() << "TimeLineGroup: minBeginOfTime set to" << stringOfDate(minBeginOfTime);
    for (int i = 0; i < items.size(); i ++)
      items[i]->setBeginOfTime(minBeginOfTime);
  } else {
    w->setBeginOfTime(minBeginOfTime);
  }

  if (w->endOfTime() > maxEndOfTime) {
    maxEndOfTime = w->endOfTime();
    for (int i = 0; i < items.size(); i ++)
      items[i]->setEndOfTime(maxEndOfTime);
  } else {
    w->setEndOfTime(maxEndOfTime);
  }

  QPair<qreal, qreal> vp(w->viewPort());
  if (vp.first < largestViewPort.first || vp.second > largestViewPort.second) {
    largestViewPort.first = std::min(largestViewPort.first, vp.first);
    largestViewPort.second = std::max(largestViewPort.second, vp.second);
    for (int i = 0; i < items.size(); i ++)
      items[i]->setViewPort(largestViewPort);
  }
  w->setViewPort(largestViewPort);

  if (! items.empty()) {
    w->setCurrentTime(items[0]->currentTime());
  }

  /* If this is a TimeLine, connect its scrolling into all other widgets.
   * Also, connect scrolling of all previous TimeLines to this widget.
   * Also, connect all other cursors to this one and the other way around. */
  connect(w, &AbstractTimeLine::currentTimeChanged,
          this, &TimeLineGroup::setAllCurrentTimes);
  connect(w, &AbstractTimeLine::beginOfTimeChanged,
          this, &TimeLineGroup::setAllBeginOfTimes);
  connect(w, &AbstractTimeLine::endOfTimeChanged,
          this, &TimeLineGroup::setAllEndOfTimes);

  for (int i = 0; i < items.size(); i ++) {
    AbstractTimeLine *w2 = items[i];

    if (w->doScroll()) {
      connect(w, &TimeLine::viewPortChanged,
              w2, &AbstractTimeLine::setViewPort);
    }
    if (w2->doScroll()) {
      connect(w2, &AbstractTimeLine::viewPortChanged,
              w, &AbstractTimeLine::setViewPort);
    }
  }

  items.append(w);
}

void TimeLineGroup::remove(AbstractTimeLine *w)
{
  for (int i = 0; i < items.size(); i++) {
    AbstractTimeLine *w2 = items[i];
    if (w2 == w) {
      items.remove(i);
      return;
    }
  }

  qWarning() << "TimeLineGroup: Asked to remove an unknown AbstractTimeLine";
}

void TimeLineGroup::setAllCurrentTimes(qreal t) const
{
  for (AbstractTimeLine *w : items) {
    if (w == sender()) continue;
    w->setCurrentTime(t);
  }
}

void TimeLineGroup::setAllBeginOfTimes(qreal t) const
{
  for (AbstractTimeLine *w : items) {
    if (w == sender()) continue;
    w->setBeginOfTime(t);
  }
}

void TimeLineGroup::setAllEndOfTimes(qreal t) const
{
  for (AbstractTimeLine *w : items) {
    if (w == sender()) continue;
    w->setEndOfTime(t);
  }
}
