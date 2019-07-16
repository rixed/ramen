#ifndef COMPILEDPROGRAMPARAM_H_190531
#define COMPILEDPROGRAMPARAM_H_190531
#include <memory>
#include <QString>
#include <string>

struct RamenValue;

struct CompiledProgramParam
{
  // For now a parameter is just a name, a value and a docstring.
  std::string name;
  std::string doc;
  std::shared_ptr<RamenValue const> val;

  CompiledProgramParam(std::string const &name_, std::string const &doc_, std::shared_ptr<RamenValue const>);
};

#endif
