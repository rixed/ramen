#include "confKey.h"

namespace conf {

Key::Key() : s() {}
Key::Key(Key const &other) : s(other.s) {}
Key::Key(std::string const &s_) : s(s_) {}
Key::~Key() {}

Key Key::null("");

bool operator<(Key const &a, Key const &b)
{
  return a.s < b.s;
}

std::ostream &operator<<(std::ostream &os, Key const &v)
{
  os << v.s;
  return os;
}

};
