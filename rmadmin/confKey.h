#ifndef CONFKEY_H_190506
#define CONFKEY_H_190506
#include <iostream>
#include <string>
#include <QMetaType>

namespace conf {

// So that Qt can serialize keys:
// FIXME: is this still necessary?

class Key
{
public:
  std::string s;
  Key() : s() {}
  Key(Key const &other) : s(other.s) {}
  Key(std::string const &s_) : s(s_) {}
  ~Key() {}

  static Key const null;
};

bool operator<(Key const &, Key const &);
bool operator==(Key const &, Key const &);
bool operator!=(Key const &, Key const &);
std::ostream &operator<<(std::ostream &, Key const &);

};

Q_DECLARE_METATYPE(conf::Key);

#endif
