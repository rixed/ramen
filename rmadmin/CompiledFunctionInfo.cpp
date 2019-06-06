#include "confValue.h"
#include "CompiledFunctionInfo.h"

CompiledFunctionInfo::CompiledFunctionInfo(QString const &name_, conf::Retention const *retention_, bool const is_lazy_, QString const &doc_, QString const &signature_) :
  name(name_),
  retention(retention_),
  is_lazy(is_lazy_),
  doc(doc_),
  signature(signature_) {}

CompiledFunctionInfo::~CompiledFunctionInfo()
{
  delete retention;
}
