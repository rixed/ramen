#ifndef CONFRCENTRYPARAM_H_190611
#define CONFRCENTRYPARAM_H_190611
#include <string>
#include "confRamenValue.h"

namespace conf {

struct RCEntryParam
{
  std::string const &name;
  conf::RamenValue const *value;

  RCEntryParam(std::string const &name_, conf::RamenValue const *value_) :
    name(name_), value(value_) {}
};

};

#endif
