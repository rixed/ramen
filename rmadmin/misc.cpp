#include <string>
#include <cmath>
#include <QDateTime>
#include <QLayout>
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

std::string const removeExt(std::string const &s, char const c)
{
  size_t i = s.rfind(c);
  if (i == std::string::npos) return s;
  return s.substr(0, i);
}

std::string const srcPathFromProgramName(std::string const &programName)
{
  size_t const i(programName.rfind('#'));
  if (i == std::string::npos) return programName;
  return programName.substr(0, i);
}

QString const removeExtQ(QString const &s, char const c)
{
  int i = s.lastIndexOf(c);
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

QString const stringOfDate(double d)
{
  // TODO: strip optional prefix
  return QDateTime::fromSecsSinceEpoch(d).toString();
}

QString const stringOfDuration(double d)
{
  QString s;

# define REDUCE(secs, unit) \
  if (d > secs) { \
    unsigned unit_ = floor(d / secs); \
    if (s.length() > 0) s += QString(", "); \
    s += QString::number(unit_) + QString(" " #unit); \
    d -= secs * unit_; \
  }

  REDUCE(86400, days);
  REDUCE(3600, hours);
  REDUCE(60, mins);
  REDUCE(1, secs);
  REDUCE(0.001, ms);

# undef REDUCE

  return s;
}

QString const stringOfBytes(size_t z)
{
  QString s;

# define REDUCE(width, symb) \
    if (z >= 1ULL << width) do { \
      size_t const u = z >> width; \
      z &= (1ULL << width) - 1; \
      if (s.length() > 0) s += ' '; \
      s += QString::number(u) + symb; \
    } while (false)

  REDUCE(60, "EiB");
  REDUCE(50, "PiB");
  REDUCE(40, "TiB");
  REDUCE(30, "GiB");
  REDUCE(20, "MiB");
  REDUCE(10, "KiB");
  REDUCE(0, "bytes");

# undef REDUCE

  return s;
}

void emptyLayout(QLayout *layout)
{
  QLayoutItem *item;
  while ((item = layout->takeAt(0)) != nullptr)
    delete item;
}

#ifdef __GNUG__
#include <cstdlib>
#include <memory>
#include <cxxabi.h>

inline std::string demangle(const char *name)
{
  int status = 0;
  std::unique_ptr<char, void(*)(void*)> res {
    abi::__cxa_demangle(name, NULL, NULL, &status), std::free
  };
  return (status==0) ? res.get() : name ;
}

#else

std::string demangle(const char* name)
{
  return name;
}

#endif

bool isClose(double v1, double v2, double prec)
{
  assert(prec > 0);

  double const magnitude = std::fmax(abs(v1), abs(v2));
  if (magnitude < prec) return true;
  double const diff = abs(v1 - v2);
  return (diff / magnitude) < prec;
}
