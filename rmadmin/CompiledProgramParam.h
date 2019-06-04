#ifndef COMPILEDPROGRAMPARAM_H_190531
#define COMPILEDPROGRAMPARAM_H_190531
#include <QString>

namespace conf {
  struct RamenValue;
};

class CompiledProgramParam
{
  // For now a parameter is just a name, a value and a docstring.
  QString name;
  QString doc;
  conf::RamenValue const *value;  // owned

public:
  CompiledProgramParam(QString const &name_, QString const &doc_, conf::RamenValue const *value);
};

#endif
