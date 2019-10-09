#include <cassert>
#include <QString>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "RamenType.h"
#include "RamenValue.h"

value RamenType::toOCamlValue() const
{
  assert(!"Don't know how to convert from a RamenType");
}

RamenValue *RamenType::valueOfQString(QString const s) const
{
  if (s.length() == 0 && nullable) return new VNull;
  return structure->valueOfQString(s);
}

// Does not alloc on OCaml heap
RamenType *RamenType::ofOCaml(value v_)
{
  assert(Is_block(v_));
  value str_ = Field(v_, 0);  // type structure
  value nul_ = Field(v_, 1);  // nullable
  assert(! Is_block(nul_));
  std::unique_ptr<RamenTypeStructure> structure(
    RamenTypeStructure::ofOCaml(str_));
  return new RamenType(std::move(structure), Bool_val(nul_));
}

std::ostream &operator<<(std::ostream &os, RamenType const &v)
{
  os << v.toQString().toStdString();
  return os;
}
