#include "RamenTypeStructure.h"

QString QStringOfRamenTypeStructure(enum RamenTypeStructure t)
{
  switch (t) {
    case EmptyType: return QString("Empty");
    case FloatType: return QString("Float");
    case StringType: return QString("String");
    case BoolType: return QString("Bool");
    case NumType: return QString("Num");
    case AnyType: return QString("Any");
    case U8Type: return QString("U8");
    case U16Type: return QString("U16");
    case U32Type: return QString("U32");
    case U64Type: return QString("U64");
    case U128Type: return QString("U128");
    case I8Type: return QString("I8");
    case I16Type: return QString("I16");
    case I32Type: return QString("I32");
    case I64Type: return QString("I64");
    case I128Type: return QString("I128");
    case EthType: return QString("Eth");
    case Ipv4Type: return QString("Ipv4");
    case Ipv6Type: return QString("Ipv6");
    case IpType: return QString("Ip");
    case Cidrv4Type: return QString("Cidrv4");
    case Cidrv6Type: return QString("Cidrv6");
    case CidrType: return QString("Cidr");
    case TupleType: return QString("Tuple");
    case VecType: return QString("Vec");
    case ListType: return QString("List");
    case RecordType: return QString("Record");
  }
  assert(!"invalid RamenTypeStructure");
}


