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

struct EventTime;
struct RamenType;
namespace conf {
  struct Retention;
};

struct CompiledFunctionInfo
{
  QString name;
  conf::Retention const *retention;  // owning, unique_ptr really
  bool is_lazy;
  QString doc;
  /* We do not even attempt at unserializing the operation, but
   * thankfully ramen stores some extra, easier info: */
  std::shared_ptr<RamenType const> outType; // as a record
  std::shared_ptr<EventTime const> eventTime;
  QStringList factors; // supposed to be a list of strings
  QString signature;
  QString in_signature;

  CompiledFunctionInfo(value);
  // Required to make this object movable (ie. to store it in vectors):
  CompiledFunctionInfo(CompiledFunctionInfo &&);
  CompiledFunctionInfo(CompiledFunctionInfo const &);
  ~CompiledFunctionInfo();
};

#endif
