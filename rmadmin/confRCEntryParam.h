#ifndef CONFRCENTRYPARAM_H_190611
#define CONFRCENTRYPARAM_H_190611
#include <string>
#include <memory>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "RamenValue.h"

namespace conf {

struct RCEntryParam
{
  std::string const name;
  std::shared_ptr<RamenValue const> val; // "value" conflicts with OCaml value type

  RCEntryParam(std::string const &name_, std::shared_ptr<RamenValue const> val_) :
    name(name_), val(val_) {}

  QString const toQString() const;

  value toOCamlValue() const;

  bool operator==(RCEntryParam const &other) const;
  bool operator!=(RCEntryParam const &other) const { return (! operator==(other)); }
};

};

#endif
