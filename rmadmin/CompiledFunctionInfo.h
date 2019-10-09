#ifndef COMPILEDFUNCTIONINFO_H_190531
#define COMPILEDFUNCTIONINFO_H_190531
#include <memory>
#include <QString>
#include <QStringList>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}

namespace conf {
  struct Retention;
};
struct RamenType;

struct CompiledFunctionInfo
{
  QString name;
  conf::Retention const *retention;  // owning, unique_ptr really
  bool is_lazy;
  QString doc;
  /* We do not even attempt at unserializing the operation, but
   * thankfully ramen stores some extra, easier info: */
  std::shared_ptr<RamenType const> out_type; // as a record
  QStringList factors; // supposed to be a list of strings
  QString signature;

  CompiledFunctionInfo(value);
  // Required to make this object movable (ie. to store it in vectors):
  CompiledFunctionInfo(CompiledFunctionInfo &&);
  CompiledFunctionInfo(CompiledFunctionInfo const &);
  ~CompiledFunctionInfo();
};

#endif
