#ifndef RAMENTYPE_H_190716
#define RAMENTYPE_H_190716
// A RamenType represents a RamenTypes.t aka a Dessser.maybe_nullable
#include <memory>
#include <QCoreApplication>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "DessserValueType.h"

struct RamenType
{
  // Shared with the RamenValue:
  std::shared_ptr<DessserValueType> vtyp;
  bool nullable;

  RamenType(std::shared_ptr<DessserValueType> structure_, bool nullable_) :
    vtyp(structure_), nullable(nullable_) {}

  RamenType(value);

  QString toQString() const
  {
    QString s(vtyp->toQString());
    if (nullable) s.append("?");
    return s;
  }

  value toOCamlValue() const;
  // Some value type can build a RamenValue from a QString:
  RamenValue *valueOfQString(QString const) const;
};

#endif
