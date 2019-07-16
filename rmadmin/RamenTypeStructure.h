#ifndef RAMENTYPESTRUCTURE_H_190716
#define RAMENTYPESTRUCTURE_H_190716
#include <QString>

// FIXME: Make this a class instead (templated for compound types)
// FIXME: Rename Type -> Struct
enum RamenTypeStructure {
  // Those constructors have no parameters, so they are integers:
  EmptyType,    // Used for errors
  FloatType,    // Use OCaml tag values for non-block structures
  StringType,
  BoolType,
  NumType,      // Not supposed to encounter that one here
  AnyType,      // Makes sense to use this for NULL values in a few places
  U8Type = 6, U16Type, U32Type, U64Type, U128Type,
  I8Type, I16Type, I32Type, I64Type, I128Type,
  EthType, Ipv4Type, Ipv6Type, IpType,
  Cidrv4Type, Cidrv6Type, CidrType,
  // Those constructors have parameters, so they are blocks which tag starts at 0:
  TupleType, VecType, ListType, RecordType
};

QString QStringOfRamenTypeStructure(enum RamenTypeStructure);

#endif
