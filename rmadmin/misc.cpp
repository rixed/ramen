#include <string>
#include <math.h>
#include <QDateTime>
#include "misc.h"

std::ostream &operator<<(std::ostream &os, int128_t const &)
{
  os << "TODO: << for int128";
  return os;
}

std::ostream &operator<<(std::ostream &os, uint128_t const &)
{
  os << "TODO: << for uint128";
  return os;
}

bool startsWith(std::string const &a, std::string const &b)
{
  if (a.length() < b.length()) return false;
  return 0 == a.compare(0, b.length(), b, 0);
}

bool endsWith(std::string const &a, std::string const &b)
{
  if (a.length() < b.length()) return false;
  return 0 == a.compare(a.length() - b.length(), b.length(), b, 0);
}

std::string const removeExt(std::string const &s)
{
  size_t i = s.rfind('.');
  if (i == std::string::npos) return s;
  return s.substr(0, i);
}

QString const removeExtQ(QString const &s)
{
  int i = s.lastIndexOf('.');
  if (i == -1) return s;
  return s.left(i);
}

bool looks_like_true(QString s_)
{
  QString s = s_.simplified();
  if (s.isEmpty() ||
      s[0] == '0' || s[0] == 'f' || s[0] == 'F') return false;
  return true;
}

QString stringOfDate(double d)
{
  // TODO: strip optional prefix
  return QDateTime::fromSecsSinceEpoch(d).toString();
}

QString stringOfDuration(double d)
{
  QString s("");

# define REDUCE(secs, unit) \
  if (d > secs) { \
    unsigned unit = floor(d / secs); \
    if (s.length() > 0) s += QString(", "); \
    s += QString::number(unit) + QString(" " #unit); \
    d -= secs * unit; \
  }

  REDUCE(86400, days);
  REDUCE(3600, hours);
  REDUCE(60, mins);
  REDUCE(1, secs);

  return s;
}
