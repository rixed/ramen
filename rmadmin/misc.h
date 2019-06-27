#ifndef MISC_H_190603
#define MISC_H_190603
#include <string>

#define SIZEOF_ARRAY(x) (sizeof(x) / sizeof(*(x)))

typedef unsigned __int128 uint128_t;
typedef __int128 int128_t;

bool startsWith(std::string const &, std::string const &);
bool endsWith(std::string const &, std::string const &);

std::string const removeExt(std::string const &);

#include <QString>

QString const removeExtQ(QString const &);

bool looks_like_true(QString);

#endif
