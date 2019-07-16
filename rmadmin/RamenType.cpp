#include <cassert>
#include <QString>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
}
#include "RamenType.h"

RamenType::RamenType(ser::ValueType type_, bool nullable_) :
  type(type_), nullable(nullable_)
{}

RamenType::RamenType() : RamenType(ser::AnyType, false) {}

value RamenType::toOCamlValue() const
{
  assert(!"Don't know how to convert from a RamenType");
}

/*bool RamenType::operator==(RamenType const &other) const
{
  RamenType const &o = static_cast<RamenType const &>(other);
  return type == o.type;
}*/

QString RamenTypeScalar::structureToQString() const
{
  switch (type) {
    case ser::FloatType:
      return QString("float");
    case ser::StringType:
      return QString("string");
    case ser::BoolType:
      return QString("boolean");
    case ser::U8Type:
      return QString("u8");
    case ser::U16Type:
      return QString("u16");
    case ser::U32Type:
      return QString("u32");
    case ser::U64Type:
      return QString("u64");
    case ser::U128Type:
      return QString("u128");
    case ser::I8Type:
      return QString("i8");
    case ser::I16Type:
      return QString("i16");
    case ser::I32Type:
      return QString("i32");
    case ser::I64Type:
      return QString("i64");
    case ser::I128Type:
      return QString("i128");
    case ser::EthType:
      return QString("eth");
    case ser::Ipv4Type:
      return QString("ipv4");
    case ser::Ipv6Type:
      return QString("ipv6");
    case ser::IpType:
      return QString("ip");
    case ser::Cidrv4Type:
      return QString("cirdv4");
    case ser::Cidrv6Type:
      return QString("cidrv6");
    case ser::CidrType:
      return QString("cidr");
    default:
      assert(!"Non scalar type");
  }
}

QString RamenTypeScalar::columnName(unsigned i) const
{
  if (i != 0) return QString();

  switch (type) {
    case ser::FloatType:
      return QString(QCoreApplication::translate("QMainWindow", "real"));
    case ser::StringType:
      return QString(QCoreApplication::translate("QMainWindow", "string"));
    case ser::BoolType:
      return QString(QCoreApplication::translate("QMainWindow", "boolean"));
    case ser::U8Type:
    case ser::U16Type:
    case ser::U32Type:
    case ser::U64Type:
    case ser::U128Type:
    case ser::I8Type:
    case ser::I16Type:
    case ser::I32Type:
    case ser::I64Type:
    case ser::I128Type:
      return QString(QCoreApplication::translate("QMainWindow", "integer"));
    case ser::EthType:
      return QString(QCoreApplication::translate("QMainWindow", "ethernet address"));
    case ser::Ipv4Type:
    case ser::Ipv6Type:
    case ser::IpType:
      return QString(QCoreApplication::translate("QMainWindow", "IP address"));
    case ser::Cidrv4Type:
    case ser::Cidrv6Type:
    case ser::CidrType:
      return QString(QCoreApplication::translate("QMainWindow", "CIDR mask"));
    default:
      return QString(QCoreApplication::translate("QMainWindow", "compound type"));
  }
}

bool RamenTypeScalar::isNumeric() const
{
  switch (type) {
    case ser::FloatType:
    case ser::U8Type:
    case ser::U16Type:
    case ser::U32Type:
    case ser::U64Type:
    case ser::U128Type:
    case ser::I8Type:
    case ser::I16Type:
    case ser::I32Type:
    case ser::I64Type:
    case ser::I128Type:
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
  RamenType(ser::RecordType, n), fields(fields_)
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
  ser::ValueType strTag =
    Is_block(str_) ?
      (ser::ValueType)(Tag_val(str_) + ser::TupleType) :
      (ser::ValueType)(Long_val(str_));
  switch (strTag) {
    case ser::EmptyType:
    case ser::FloatType:
    case ser::StringType:
    case ser::BoolType:
    case ser::AnyType:
    case ser::U8Type:
    case ser::U16Type:
    case ser::U32Type:
    case ser::U64Type:
    case ser::U128Type:
    case ser::I8Type:
    case ser::I16Type:
    case ser::I32Type:
    case ser::I64Type:
    case ser::I128Type:
    case ser::EthType:
    case ser::Ipv4Type:
    case ser::Ipv6Type:
    case ser::IpType:
    case ser::Cidrv4Type:
    case ser::Cidrv6Type:
    case ser::CidrType:
      ret = new RamenTypeScalar(strTag, Bool_val(nul_));
      break;
    case ser::TupleType:
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
    case ser::VecType:
      {
        std::shared_ptr<RamenType const> subType(ramenTypeOfOCaml(Field(str_, 1)));
        ret = new RamenTypeVec(Long_val(Field(str_, 0)), subType, Bool_val(nul_));
      }
      break;
    case ser::ListType:
      {
        std::shared_ptr<RamenType const> subType(ramenTypeOfOCaml(Field(str_, 0)));
        ret = new RamenTypeList(subType, Bool_val(nul_));
      }
      break;
    case ser::RecordType:
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


