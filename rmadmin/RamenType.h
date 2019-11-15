#ifndef RAMENTYPE_H_190716
#define RAMENTYPE_H_190716
// A RamenType represents a RamenTypes.t.
#include <memory>
#include <QCoreApplication>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "RamenTypeStructure.h"

struct RamenType
{
  // Shared with the RamenValue:
  std::shared_ptr<RamenTypeStructure> structure;
  bool nullable;

  RamenType(std::shared_ptr<RamenTypeStructure> structure_, bool nullable_) :
    structure(structure_), nullable(nullable_) {}

  RamenType(value);

  // FIXME: useful?
  RamenType() : RamenType(std::shared_ptr<RamenTypeStructure>(new TEmpty), false) {}

  QString toQString() const
  {
    QString s(structure->toQString());
    if (nullable) s.append("?");
    return s;
  }

  value toOCamlValue() const;
  // Some structure can build a RamenValue from a QString:
  RamenValue *valueOfQString(QString const) const;
};

#endif
