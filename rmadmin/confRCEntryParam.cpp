#include <cassert>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# undef alloc
# undef flush
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
  return QString::fromStdString(name) + QString('=') +
         val->toQString(std::string());
}

bool RCEntryParam::operator==(RCEntryParam const &other) const
{
  if (name != other.name) return false;

  if (val) {
    if (! other.val || *val != *other.val) return false;
  } else {
    if (other.val) return false;
  }

  return true;
}

};
