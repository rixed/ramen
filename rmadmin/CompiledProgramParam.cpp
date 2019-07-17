#include "RamenValue.h"
#include "CompiledProgramParam.h"

CompiledProgramParam::CompiledProgramParam(std::string const &name_, std::shared_ptr<RamenType const> type_, std::string const &doc_, std::shared_ptr<RamenValue const> val_) :
  name(name_), type(type_), doc(doc_), val(val_) {}
