#ifndef CONFKEY_H_190506
#define CONFKEY_H_190506
#include <iostream>
#include <string>
#include <QMetaType>
#include <QDebug>

namespace conf {

class Key
{
public:
  std::string s;
  Key();
  Key(Key const &other);
  Key(std::string const &);
  ~Key();
};

bool operator<(Key const &, Key const &);
std::ostream &operator<<(std::ostream &, Key const &);
QDebug operator<<(QDebug, Key const &);

};

Q_DECLARE_METATYPE(conf::Key);

#endif
