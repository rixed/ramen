#include <cassert>
#include <QString>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
}
#include "RamenType.h"

RamenType::RamenType(RamenTypeStructure structure_, bool nullable_) :
  structure(structure_), nullable(nullable_)
{}

value RamenType::toOCamlValue() const
{
  assert(!"Don't know how to convert from a RamenType");
}

QString RamenTypeScalar::structureToQString() const
{
  switch (structure) {
    case FloatType:
      return QString("float");
    case StringType:
      return QString("string");
    case BoolType:
      return QString("boolean");
    case U8Type:
      return QString("u8");
    case U16Type:
      return QString("u16");
    case U32Type:
      return QString("u32");
    case U64Type:
      return QString("u64");
    case U128Type:
      return QString("u128");
    case I8Type:
      return QString("i8");
    case I16Type:
      return QString("i16");
    case I32Type:
      return QString("i32");
    case I64Type:
      return QString("i64");
    case I128Type:
      return QString("i128");
    case EthType:
      return QString("eth");
    case Ipv4Type:
      return QString("ipv4");
    case Ipv6Type:
      return QString("ipv6");
    case IpType:
      return QString("ip");
    case Cidrv4Type:
      return QString("cirdv4");
    case Cidrv6Type:
      return QString("cidrv6");
    case CidrType:
      return QString("cidr");
    default:
      assert(!"Non scalar type");
  }
}

QString RamenTypeScalar::columnName(unsigned i) const
{
  if (i != 0) return QString();

  switch (structure) {
    case FloatType:
      return QString(QCoreApplication::translate("QMainWindow", "real"));
    case StringType:
      return QString(QCoreApplication::translate("QMainWindow", "string"));
    case BoolType:
      return QString(QCoreApplication::translate("QMainWindow", "boolean"));
    case U8Type:
    case U16Type:
    case U32Type:
    case U64Type:
    case U128Type:
    case I8Type:
    case I16Type:
    case I32Type:
    case I64Type:
    case I128Type:
      return QString(QCoreApplication::translate("QMainWindow", "integer"));
    case EthType:
      return QString(QCoreApplication::translate("QMainWindow", "ethernet address"));
    case Ipv4Type:
    case Ipv6Type:
    case IpType:
      return QString(QCoreApplication::translate("QMainWindow", "IP address"));
    case Cidrv4Type:
    case Cidrv6Type:
    case CidrType:
      return QString(QCoreApplication::translate("QMainWindow", "CIDR mask"));
    default:
      return QString(QCoreApplication::translate("QMainWindow", "compound type"));
  }
}

bool RamenTypeScalar::isNumeric() const
{
  switch (structure) {
    case FloatType:
    case U8Type:
    case U16Type:
    case U32Type:
    case U64Type:
    case U128Type:
    case I8Type:
    case I16Type:
    case I32Type:
    case I64Type:
    case I128Type:
      return true;
    default:
      return false;
  }
}

QString RamenTypeTuple::structureToQString() const
{
  QString s("(");
  for (size_t i = 0; i < fields.size(); i++) {
    if (s.length() > 1) s.append("; ");
    s.append(fields[i]->toQString());
  }
  s.append(")");
  return s;
}

QString RamenTypeVec::structureToQString() const
{
  return subType->toQString() + "[" + QString::number(dim) + "]";
}

QString RamenTypeList::structureToQString() const
{
  return subType->toQString() + "[]";
}

RamenTypeRecord::RamenTypeRecord(std::vector<std::pair<QString, std::shared_ptr<RamenType const>>> fields_, bool n) :
  RamenType(RecordType, n), fields(fields_)
{
  assert(fields.size() < 65536);
  serOrder.reserve(fields.size());

  for (uint16_t o = 0; o < (uint16_t)fields.size(); o++) {
    serOrder.push_back(o);
  }

  std::sort(serOrder.begin(), serOrder.end(), [this](uint16_t a, uint16_t b) {
    return fields[a].first < fields[b].first;
  });
}

QString RamenTypeRecord::structureToQString() const
{
  QString s("{");
  for (size_t i = 0; i < fields.size(); i++) {
    if (s.length() > 1) s.append("; ");
    s.append(fields[i].first);
    s.append(":");
    s.append(fields[i].second->toQString());
  }
  s.append("}");
  return s;
}

RamenType *ramenTypeOfOCaml(value v_)
{
  CAMLparam1(v_);
  CAMLlocal3(tmp_, str_, nul_);
  assert(Is_block(v_));
  RamenType *ret = nullptr;
  str_ = Field(v_, 0);  // type structure
  nul_ = Field(v_, 1);  // nullable
  RamenTypeStructure strTag =
    Is_block(str_) ?
      (RamenTypeStructure)(Tag_val(str_) + TupleType) :
      (RamenTypeStructure)(Long_val(str_));
  switch (strTag) {
    case EmptyType:
    case FloatType:
    case StringType:
    case BoolType:
    case NumType:
    case AnyType:
    case U8Type:
    case U16Type:
    case U32Type:
    case U64Type:
    case U128Type:
    case I8Type:
    case I16Type:
    case I32Type:
    case I64Type:
    case I128Type:
    case EthType:
    case Ipv4Type:
    case Ipv6Type:
    case IpType:
    case Cidrv4Type:
    case Cidrv6Type:
    case CidrType:
      ret = new RamenTypeScalar(strTag, Bool_val(nul_));
      break;
    case TupleType:
      {
        tmp_ = Field(str_, 0);
        assert(Is_block(tmp_));  // an array of types
        std::vector<std::shared_ptr<RamenType const>> fields;
        unsigned const num_fields = Wosize_val(tmp_);
        fields.reserve(num_fields);
        for (unsigned f = 0; f < num_fields; f++) {
          fields.emplace_back(ramenTypeOfOCaml(Field(tmp_, f)));
        }
        ret = new RamenTypeTuple(fields, Bool_val(nul_));
      }
      break;
    case VecType:
      {
        std::shared_ptr<RamenType const> subType(ramenTypeOfOCaml(Field(str_, 1)));
        ret = new RamenTypeVec(Long_val(Field(str_, 0)), subType, Bool_val(nul_));
      }
      break;
    case ListType:
      {
        std::shared_ptr<RamenType const> subType(ramenTypeOfOCaml(Field(str_, 0)));
        ret = new RamenTypeList(subType, Bool_val(nul_));
      }
      break;
    case RecordType:
      {
        tmp_ = Field(str_, 0);
        assert(Is_block(tmp_));  // an array of name * type pairs
        std::vector<std::pair<QString, std::shared_ptr<RamenType const>>> fields;
        unsigned const num_fields = Wosize_val(tmp_);
        fields.reserve(num_fields);
        for (unsigned f = 0; f < num_fields; f++) {
          str_ = Field(tmp_, f);
          assert(Is_block(str_));   // a pair of string * type
          fields.emplace_back(
            QString(String_val(Field(str_, 0))),
            ramenTypeOfOCaml(Field(str_, 1))
          );
          //std::cout << "Record type field#" << f <<" is " << fields.back().first.toStdString() << std::endl;
        }
        ret = new RamenTypeRecord(fields, Bool_val(nul_));
      }
      break;
  }
  CAMLreturnT(RamenType *, ret);
}

std::ostream &operator<<(std::ostream &os, RamenType const &v)
{
  os << v.toQString().toStdString();
  return os;
}


