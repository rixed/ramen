#ifndef CONFKEY_H_190506
#define CONFKEY_H_190506
#include <iostream>
#include <string>
#include <QMetaType>

namespace conf {

/* All this is required so that we can enqueue objects in the kvs QMap, which
 * will fail at runtime (with an error message) whenever one tries to add
 * a non-qmetatyped thing. */

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
