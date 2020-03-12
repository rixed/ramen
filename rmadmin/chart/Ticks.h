#ifndef TICKS_H_200311
#define TICKS_H_200311
#include <vector>
#include <QString>
#include <QtGlobal>

inline qreal logOfBase(int base, qreal x)
{
  return std::log(x) / std::log(base);
}

struct Tick {
  qreal pos;
  bool major;
  QString label;

  Tick(qreal p, bool m, QString const &l)
    : pos(p), major(m), label(l) {}
};

struct Ticks {
  std::vector<Tick> ticks;

  Ticks(qreal min, qreal max, bool log = false, int base = 10);
};

#endif
