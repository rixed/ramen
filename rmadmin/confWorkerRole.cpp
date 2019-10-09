#include <cassert>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "confWorkerRole.h"

namespace conf {

WorkerRole *WorkerRole::ofOCamlValue(value v_)
{
  CAMLparam1(v_);
  WorkerRole *ret;
  if (Is_block(v_)) { // top half
    assert(Tag_val(v_) == 0);
    // TODO: read that block
    ret = new WorkerRole(true);
  } else {
    assert(Long_val(v_) == 0);
    ret = new WorkerRole(false);
  }
  CAMLreturnT(WorkerRole *, ret);
}

QString const WorkerRole::toQString() const
{
  if (isTopHalf) return QString("top-half");
  else return QString("normal");
}

};
