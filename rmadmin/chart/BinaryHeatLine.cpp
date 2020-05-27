#include <QDebug>
#include <QPainter>
#include <QPaintEvent>
#include <QRect>
#include "confValue.h"
#include "misc.h"
#include "chart/BinaryHeatLine.h"

BinaryHeatLine::BinaryHeatLine(
    qreal beginOftime, qreal endOfTime,
    bool withCursor,
    bool doScroll,
    QWidget *parent)
  : HeatLine(beginOftime, endOfTime, withCursor, doScroll, parent)
{
}

void BinaryHeatLine::add(qreal start, qreal stop)
{
  /* Cannot be Qt::black because of this bug:
   * https://bugreports.qt.io/browse/QTBUG-9343 */
  HeatLine::add(start, stop, QColor(25, 25, 25));
}

void BinaryHeatLine::setArchivedTimes(conf::TimeRange const &archivedTimes)
{
  reset();
  for (conf::TimeRange::Range const &r : archivedTimes.range) {
    if (r.t1 >= r.t2) {
      qWarning() << "BinaryHeatLine: Skip invalid range "
                 << stringOfDate(r.t1) << "..." << stringOfDate(r.t2);
      continue;
    }
    add(r.t1, r.t2);
  }
  update();
}
