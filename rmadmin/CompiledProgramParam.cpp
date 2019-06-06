#include "confRamenValue.h"
#include "CompiledProgramParam.h"

CompiledProgramParam::CompiledProgramParam(QString const &name_, QString const &doc_, conf::RamenValue const *value_) :
  name(name_), doc(doc_), value(value_) {}

CompiledProgramParam::~CompiledProgramParam()
{
  delete value;
}
