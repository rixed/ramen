#include <cassert>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
}
#include "confWorkerRef.h"

namespace conf {

WorkerRef *WorkerRef::ofOCamlValue(value v_)
{
  CAMLparam1(v_);
  WorkerRef *ret;
  assert(Is_block(v_));
  assert(Wosize_val(v_) == 3);
  ret = new WorkerRef(String_val(Field(v_, 0)),
                      String_val(Field(v_, 1)),
                      String_val(Field(v_, 2)));
  CAMLreturnT(WorkerRef *, ret);
}

};
