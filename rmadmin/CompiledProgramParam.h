#ifndef COMPILEDPROGRAMPARAM_H_190531
#define COMPILEDPROGRAMPARAM_H_190531
#include <memory>
#include <QString>
#include <string>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}

struct RamenValue;
struct RamenType;

struct CompiledProgramParam
{
  // For now a parameter is just a name, a value and a docstring.
  std::string name;
  std::shared_ptr<RamenType const> type;
  std::shared_ptr<RamenValue const> val;
  std::string doc;

  CompiledProgramParam(value);
};

#endif
