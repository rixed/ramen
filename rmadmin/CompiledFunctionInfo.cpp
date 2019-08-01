extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
}
#include "confValue.h"
#include "RamenType.h"
#include "CompiledFunctionInfo.h"

// Does not alloc on OCaml heap
CompiledFunctionInfo::CompiledFunctionInfo(QString const &name_, conf::Retention const *retention_, bool const is_lazy_, QString const &doc_, std::shared_ptr<RamenType const> out_type_, QStringList const &factors_, QString const &signature_) :
  name(name_),
  retention(retention_),
  is_lazy(is_lazy_),
  doc(doc_),
  out_type(out_type_),
  factors(factors_),
  signature(signature_) {}

CompiledFunctionInfo::CompiledFunctionInfo(value v_)
{
  value tmp_ = Field(v_, 1);  // the (optional) retention
  retention = nullptr;
  if (Is_block(tmp_)) {
    tmp_ = Field(tmp_, 0);
    assert(Tag_val(tmp_) == Double_array_tag);
    retention =
      new conf::Retention(Double_field(tmp_, 0), Double_field(tmp_, 1));
  }
  name = String_val(Field(v_, 0));
  is_lazy = Bool_val(Field(v_, 2));
  doc = String_val(Field(v_, 3));
  // field 4 is the operation, which is too hard to parse. Hopefully we have those:
  out_type = std::shared_ptr<RamenType const>(RamenType::ofOCaml(Field(v_, 5)));
  for (tmp_ = Field(v_, 6); Is_block(tmp_); tmp_ = Field(tmp_, 1)) {
    factors.append(QString(String_val(Field(tmp_, 0))));
  }
  signature = String_val(Field(v_, 7));
}

CompiledFunctionInfo::~CompiledFunctionInfo()
{
  delete retention;
}
