#include <cassert>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
}
#include "confRCEntryParam.h"

namespace conf {

// This _does_ alloc on the OCaml heap
value RCEntryParam::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc_tuple(2);
  Store_field(ret, 0, caml_copy_string(name.c_str()));
  Store_field(ret, 1, val->toOCamlValue());
  CAMLreturn(ret);
}

QString const RCEntryParam::toQString() const
{
  return QString::fromStdString(name) + QString('=') + val->toQString();
}

};
