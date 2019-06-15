#include <cassert>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
}
#include "confRCEntryParam.h"

namespace conf {

value RCEntryParam::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, lst);
  ret = caml_alloc_tuple(2);
  Store_field(ret, 0, caml_copy_string(name.c_str()));
  Store_field(ret, 1, val->toOCamlValue());
  CAMLreturn(ret);
}

};
