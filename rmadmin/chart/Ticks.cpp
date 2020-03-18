#include <cassert>
#include <cmath>
#include <QDebug>

#include "chart/Ticks.h"

extern inline qreal logOfBase(int base, qreal x);

static qreal valueOfPos(qreal p, bool log, int base)
{
  if (log) {
    return std::pow(base, p);
  } else {
    return p;
  }
}

static QString labelOfPos(qreal p, bool log, int base)
{
  if (log) {
    double iptr;
    if (0 == std::modf(p, &iptr)) {
      return QString("%1^%2").arg(base).arg(p);
    } else {
      return QString("%1").arg(std::pow(base, p));
    }
  } else {
    return QString("%1").arg(p);
  }
}

Ticks::Ticks(qreal min_, qreal max_, bool log, int base)
{
  qreal const min(log ? logOfBase(base, min_) : min_);
  qreal const max(log ? logOfBase(base, max_) : max_);

  qreal const d(max - min);
  assert(d > 0);
  qreal const dist(
    std::pow(base, std::round(logOfBase(base, d))));
  qreal const subDist(dist / base);

  qreal p(dist * std::floor(min / dist));
  qDebug() << "Ticks: min=" << min << "max=" << max << "dist=" << dist << "p=" << p;

  for (int i = 0; i < base + 2; i++) {
    ticks.emplace_back(valueOfPos(p, log, base), true, labelOfPos(p, log, base));
    qreal q(p + subDist);
    for (int j = 1; j < base; j++) {
      ticks.emplace_back(valueOfPos(q, log, base), false, labelOfPos(q, log, base));
      q += subDist;
    }
    p += dist;
    if (p > max + dist) break;
  }
}