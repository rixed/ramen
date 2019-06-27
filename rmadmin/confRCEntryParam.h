#ifndef CONFRCENTRYPARAM_H_190611
#define CONFRCENTRYPARAM_H_190611
#include <string>
#include <memory>
extern "C" {
# include <caml/mlvalues.h>
}
#include "confRamenValue.h"

namespace conf {

struct RCEntryParam
{
  std::string const name;
  std::shared_ptr<conf::RamenValue const> val; // "value" conflicts with OCaml value type

  RCEntryParam(std::string const &name_, std::shared_ptr<conf::RamenValue const> val_) :
    name(name_), val(val_) {}

  value toOCamlValue() const;
};

};

#endif
