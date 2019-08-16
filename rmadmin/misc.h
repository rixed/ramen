#ifndef MISC_H_190603
#define MISC_H_190603
#include <string>
#include <iostream>

#define SIZEOF_ARRAY(x) (sizeof(x) / sizeof(*(x)))

typedef unsigned __int128 uint128_t;
typedef __int128 int128_t;

bool startsWith(std::string const &, std::string const &);
bool endsWith(std::string const &, std::string const &);

std::string const removeExt(std::string const &);

std::ostream &operator<<(std::ostream &, int128_t const &);
std::ostream &operator<<(std::ostream &, uint128_t const &);

#include <QString>

QString const removeExtQ(QString const &);

bool looks_like_true(QString);

QString const stringOfDate(double);
QString const stringOfDuration(double);
QString const stringOfBytes(size_t);

class QLayout;

void emptyLayout(QLayout *);

std::string demangle(const char *);

/* There are a few global variables that are used if not NULL. When they are
 * deleted, the global variable has to be invalidated before destruction
 * begins. */
template<class T>
void danceOfDel(T *t)
{
  if (! t) return;

  T *tmp = t;
  t = nullptr;
  delete tmp;
}

#endif
