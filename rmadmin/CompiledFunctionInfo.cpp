extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
# undef alloc
# undef flush
}
#include "CompiledFunctionInfo.h"
#include "confValue.h"
#include "EventTime.h"
#include "RamenType.h"

// Does not alloc on OCaml heap
CompiledFunctionInfo::CompiledFunctionInfo(value v_) :
  retention(nullptr)
{
  assert(Is_block(v_));
  assert(Wosize_val(v_) == 9);
  value tmp_ = Field(v_, 1);  // the (optional) retention
  if (Is_block(tmp_)) {
    retention = new conf::Retention(Field(tmp_, 0));
  }
  name = String_val(Field(v_, 0));
  is_lazy = Bool_val(Field(v_, 2));
  doc = String_val(Field(v_, 3));
  // field 4 is the operation, which is too hard to parse. Hopefully we have those:
  outType = std::make_shared<RamenType const>(Field(v_, 5));
  for (tmp_ = Field(v_, 6); Is_block(tmp_); tmp_ = Field(tmp_, 1)) {
    factors.append(QString(String_val(Field(tmp_, 0))));
  }
  eventTime = std::make_shared<EventTime const>(*outType);
  signature = String_val(Field(v_, 7));
  in_signature = String_val(Field(v_, 8));
}

CompiledFunctionInfo::CompiledFunctionInfo(CompiledFunctionInfo &&other) :
  name(std::move(other.name)),
  is_lazy(other.is_lazy),
  doc(std::move(other.doc)),
  outType(other.outType),
  factors(std::move(other.factors)),
  signature(std::move(other.signature)),
  in_signature(std::move(other.in_signature))
{
  retention = other.retention;
  other.retention = nullptr;
}

CompiledFunctionInfo::CompiledFunctionInfo(CompiledFunctionInfo const &other) :
  name(other.name),
  is_lazy(other.is_lazy),
  doc(other.doc),
  outType(other.outType),
  factors(other.factors),
  signature(other.signature),
  in_signature(other.in_signature)
{
  if (other.retention) {
    retention = new conf::Retention(*other.retention);
  } else {
    retention = nullptr;
  }
}

CompiledFunctionInfo::~CompiledFunctionInfo()
{
  if (retention) delete retention;
}
