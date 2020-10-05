#ifndef MISC_H_190603
#define MISC_H_190603
#include <string>

#define SIZEOF_ARRAY(x) (sizeof(x) / sizeof(*(x)))

typedef unsigned __int128 uint128_t;
typedef __int128 int128_t;

bool startsWith(std::string const &, std::string const &);
bool endsWith(std::string const &, std::string const &);

// Remove everything after and including the last occurrence of the given char
std::string const removeExt(std::string const &, char const);

// Remove the optional program name suffix:
std::string const srcPathFromProgramName(std::string const &);
// The other way around: extract the suffix from a program name
std::string const suffixFromProgramName(std::string const &);

std::ostream &operator<<(std::ostream &, int128_t const &);
std::ostream &operator<<(std::ostream &, uint128_t const &);

#include <QString>

QString const removeExtQ(QString const &, char const);

/* Thanks to KDE bug where menu short-cuts are added: */
QString const removeAmp(QString const &);

bool looks_like_true(QString);

QString const stringOfDate(double);
QString const stringOfDuration(double);
QString const stringOfBytes(size_t);
QString const stringOfBool(bool);

QString const abbrev(int, QString const &);

class QLayout;

void emptyLayout(QLayout *);

std::string demangle(const char *);

/* There are a few global variables that are used if not NULL. When they are
 * deleted, the global variable has to be invalidated before destruction
 * begins. */
template<class T>
void danceOfDel(T **t)
{
  if (! *t) return;

  T *tmp = *t;
  *t = nullptr;
  delete tmp;
}

template<class T>
void danceOfDelLater(T **t)
{
  if (! *t) return;

  T *tmp = *t;
  *t = nullptr;
  tmp->deleteLater();
}

/* Don't be too strict when comparing edited values for equality: */

bool isClose(double v1, double v2, double prec = 1e-6);

// Expand a tree view recursively from a parent:
class QModelIndex;
class QTreeView;
void expandAllFromParent(QTreeView *, QModelIndex const &, int first, int last);

inline bool overlap(double t1, double t2, double u1, double u2)
{
  return u1 < t2 && u2 > t1;
}

#include <QDateTime>

inline double getTime()
{
  return 0.001 * QDateTime::currentDateTime().toMSecsSinceEpoch();
}

#include <QColor>

/* Returns a color made of a mix of c1 and c2 (100% c1 if ratio is 0 and
 * 100% c2 if ratio is 1) */
QColor blendColor(QColor const &c1, QColor const &c2, double ratio);

#define WITH_BETA_FEATURES (!qgetenv("RMADMIN_BETA").isEmpty())

#endif
