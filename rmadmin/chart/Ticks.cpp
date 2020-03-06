#include <cassert>
#include <cmath>
#include <QDebug>

#include "chart/Ticks.h"

static QString labelOfPos(qreal p, int)
{
  return QString("%1").arg(p);
}

Ticks::Ticks(qreal min, qreal max, int base)
{
  qreal const d(max - min);
  assert(d > 0);
  qreal const dist(
    std::pow(base, std::round((std::log(d) / std::log(base)))));
  qreal const subDist(dist / base);
  qreal p(dist * std::floor(min / dist));
  qDebug() << "Ticks: min=" << min << "max=" << max << "dist=" << dist << "p=" << p;

  for (int i = 0; i < base + 2; i++) {
    ticks.emplace_back(p, true, labelOfPos(p, base));
    qreal q(p + subDist);
    for (int j = 1; j < base; j++) {
      ticks.emplace_back(q, false, labelOfPos(q, base));
      q += subDist;
    }
    p += dist;
    if (p > max + dist) break;
  }
}
