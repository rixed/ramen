#include <cassert>
#include <QString>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
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

RamenType *RamenType::ofOCaml(value v_)
{
  CAMLparam1(v_);
  CAMLlocal2(str_, nul_);
  assert(Is_block(v_));
  RamenType *ret = nullptr;
  str_ = Field(v_, 0);  // type structure
  nul_ = Field(v_, 1);  // nullable
  assert(! Is_block(nul_));
  std::unique_ptr<RamenTypeStructure> structure(
    RamenTypeStructure::ofOCaml(str_));
  ret = new RamenType(std::move(structure), Bool_val(nul_));
  CAMLreturnT(RamenType *, ret);
}

std::ostream &operator<<(std::ostream &os, RamenType const &v)
{
  os << v.toQString().toStdString();
  return os;
}
