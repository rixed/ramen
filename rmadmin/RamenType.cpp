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
  return vtyp->valueOfQString(s);
}

// Does not alloc on OCaml heap
RamenType::RamenType(value v_)
{
  assert(Is_block(v_));
  assert(Wosize_val(v_) == 2);
  value vtyp_ = Field(v_, 0);  // value type
  value nul_ = Field(v_, 1);  // nullable
  assert(! Is_block(nul_));
  vtyp =
    std::shared_ptr<DessserValueType>(DessserValueType::ofOCaml(vtyp_));
  nullable = Bool_val(nul_);
}
