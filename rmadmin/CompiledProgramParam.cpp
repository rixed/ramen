extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "RamenValue.h"
#include "RamenType.h"
#include "CompiledProgramParam.h"

CompiledProgramParam::CompiledProgramParam(value v_)
{
  value ptyp_ = Field(v_, 0);  // the ptyp field
  name = String_val(Field(ptyp_, 0));
  type = std::make_shared<RamenType const>(Field(ptyp_, 1));
  val = std::shared_ptr<RamenValue const>(RamenValue::ofOCaml(Field(v_, 1)));
  doc = String_val(Field(ptyp_, 3));
}
