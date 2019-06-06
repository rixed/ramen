#ifndef COMPILEDFUNCTIONINFO_H_190531
#define COMPILEDFUNCTIONINFO_H_190531
#include <QString>

namespace conf {
  struct Retention;
};

struct CompiledFunctionInfo
{
  QString name;
  conf::Retention const *retention; // maybe null, owned
  bool is_lazy;
  QString doc;
  QString signature;

  CompiledFunctionInfo(QString const &name_, conf::Retention const *, bool const is_lazy_, QString const &doc_, QString const &signature_);
  ~CompiledFunctionInfo();
};

#endif
