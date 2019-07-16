#include "RamenValue.h"
#include "CompiledProgramParam.h"

CompiledProgramParam::CompiledProgramParam(std::string const &name_, std::string const &doc_, std::shared_ptr<RamenValue const> val_) :
  name(name_), doc(doc_), val(val_) {}
