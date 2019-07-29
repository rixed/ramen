#ifndef CONFKEY_H_190506
#define CONFKEY_H_190506
#include <iostream>
#include <string>
#include <QMetaType>

namespace conf {

// So that Qt can serialize keys:

class Key
{
public:
  std::string s;
  Key();
  Key(Key const &other);
  Key(std::string const &);
  ~Key();

  static Key null;
};

bool operator<(Key const &, Key const &);
std::ostream &operator<<(std::ostream &, Key const &);

};

Q_DECLARE_METATYPE(conf::Key);

#endif
