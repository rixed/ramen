#ifndef COMPILEDFUNCTIONINFO_H_190531
#define COMPILEDFUNCTIONINFO_H_190531
#include <memory>
#include <QString>
#include <QStringList>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
}

namespace conf {
  struct Retention;
};
struct RamenType;

struct CompiledFunctionInfo
{
  QString name;
  conf::Retention const *retention; // maybe null, owned
  bool is_lazy;
  QString doc;
  /* We do not even attempt at unserializing the operation, but
   * thankfully ramen stores some extra, easier info: */
  std::shared_ptr<RamenType const> out_type; // as a record
  QStringList factors; // supposed to be a list of strings
  QString signature;

  CompiledFunctionInfo(QString const &name, conf::Retention const *, bool const is_lazy, QString const &doc, std::shared_ptr<RamenType const> out_type, QStringList const &factors, QString const &signature);
  CompiledFunctionInfo(value);

  ~CompiledFunctionInfo();
};

#endif
