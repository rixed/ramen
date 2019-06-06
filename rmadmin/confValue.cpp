#include <cassert>
#include <string.h>
#include <cstdlib>
#include <cstring>
#include <QtWidgets>
#include <QString>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
}
#include "misc.h"
#include "confRamenValue.h"
#include "confValue.h"

namespace conf {

static QString const stringOfValueType(ValueType valueType)
{
  static QString const stringOfValueTypes[] = {
    [BoolType] = "BoolType",
    [IntType] = "IntType",
    [FloatType] = "FloatType",
    [StringType] = "StringType",
    [ErrorType] = "ErrorType",
    [WorkerType] = "WorkerType",
    [RetentionType] = "RetentionType",
    [TimeRangeType] = "TimeRangeType",
    [TupleType] = "TupleType",
    [RamenTypeType] = "RamenTypeType",
    [RamenValueType] = "RamenValueType",
    [TargetConfigType] = "TargetConfigType",
    [SourceInfoType] = "SourceInfoType",
    [LastValueType] = "LastValueType",
  };
  assert((size_t)valueType < SIZEOF_ARRAY(stringOfValueTypes));
  return stringOfValueTypes[(size_t)valueType];
}

Value::Value()
{
  valueType = LastValueType;
}

Value::Value(ValueType valueType_) : valueType(valueType_) {}

Value::~Value() {}

QString Value::toQString() const
{
  return QString("TODO: toQString for ") + stringOfValueType(valueType);
}

value Value::toOCamlValue() const
{
  assert(!"Don't know how to convert from a base Value");
}

bool Value::operator==(Value const &other) const
{
  return valueType == other.valueType;
}

bool Value::operator!=(Value const &other) const
{
  return !operator==(other);
}

static RamenType *ramenTypeOfOCaml(value v_)
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

Value *valueOfOCaml(value v_)
{
  CAMLparam1(v_);
  CAMLlocal3(tmp1_, tmp2_, tmp3_);
  assert(Is_block(v_));
  ValueType valueType = (ValueType)Tag_val(v_);
  Value *ret = nullptr;
  switch (valueType) {
    case BoolType:
      ret = new Bool(Bool_val(Field(v_, 0)));
      break;
    case IntType:
      ret = new Int(Int64_val(Field(v_, 0)));
      break;
    case FloatType:
      ret = new Float(Double_val(Field(v_, 0)));
      break;
    case StringType:
      ret = new String(String_val(Field(v_, 0)));
      break;
    case ErrorType:
      ret = new Error(
        Double_val(Field(v_, 0)),
        (unsigned)Int_val(Field(v_, 1)),
        String_val(Field(v_, 2)));
      break;
    case WorkerType:
      ret = new Worker(
        String_val(Field(v_, 0)),
        String_val(Field(v_, 1)),
        String_val(Field(v_, 2)));
      break;
    case RetentionType:
      tmp1_ = Field(v_, 0);
      assert(Tag_val(tmp1_) == Double_array_tag);
      ret = new Retention(
        Double_field(tmp1_, 0),
        Double_field(tmp1_, 1));
      break;
    case TimeRangeType:
      {
        std::vector<std::pair<double, double>> range;
        range.reserve(5);
        // TODO: read that value
        range.push_back(std::pair<double, double>(1., 2.));
        ret = new TimeRange(range);
      }
      break;
    case TupleType:
      ret = new Tuple(
        // If the value is <0 it must have wrapped around in the OCaml side.
        Int_val(Field(v_, 0)),
        Bytes_val(Field(v_, 1)),
        caml_string_length(Field(v_, 1)));
      break;
    case RamenTypeType:
      ret = ramenTypeOfOCaml(Field(v_, 0));
      break;
    case RamenValueType:
      ret = new RamenValueValue(
        RamenValueOfOCaml(Field(v_, 0)));
      break;
    case TargetConfigType:
      assert(!"TODO: TargetConfigType of OCaml");
    case SourceInfoType:
      {
        v_ = Field(v_, 0);
        QString md5(String_val(Field(v_, 0)));
        v_ = Field(v_, 1);
        switch (Tag_val(v_)) {
          case 0: // CompiledSourceInfo
            {
              v_ = Field(v_, 0);
              // `Some expression` for the running condition
              bool hasRunCond = Is_block(Field(v_, 1));
              SourceInfo *sourceInfo = new SourceInfo(md5, hasRunCond);
              ret = sourceInfo;
              // Iter over the cons cells of the RamenTuple.params:
              for (tmp1_ = Field(v_, 0); Is_block(tmp1_); tmp1_ = Field(tmp1_, 1)) {
                tmp2_ = Field(tmp1_, 0);  // the RamenTuple.param
                tmp3_ = Field(tmp2_, 0);  // the ptyp field
                CompiledProgramParam *p =
                  new CompiledProgramParam(
                    String_val(Field(tmp3_, 0)),  // name
                    String_val(Field(tmp3_, 3)),  // doc
                    RamenValueOfOCaml(Field(tmp2_, 1))); // value
                sourceInfo->addParam(p);
              }
              // TODO: Same for funcs -> CompiledFunctionInfo
            }
            break;
          case 1: // FailedSourceInfo
            v_ = Field(v_, 0);
            ret = new SourceInfo(md5, String_val(Field(v_, 0)));
            break;
          default:
            assert(!"Not a detail_source_info?!");
        }
      }
      break;
    case LastValueType:
    default:
      assert(!"Tag_val(v_) <= LastValueType");
  }
  CAMLreturnT(Value *, ret);
}

static bool looks_like_true(QString s_)
{
  QString s = s_.simplified();
  if (s.isEmpty() ||
      s[0] == '0' || s[0] == 'f' || s[0] == 'F') return false;
  return true;
}

Value *valueOfQString(ValueType vt, QString const &s)
{
  bool ok = true;
  Value *ret = nullptr;
  switch (vt) {
    case BoolType:
      ret = new Bool(looks_like_true(s));
      break;
    case IntType:
      ret = new Int(s.toLong(&ok));
      break;
    case FloatType:
      ret = new Float(s.toDouble(&ok));
      break;
    case StringType:
      ret = new String(s);
      break;
    case ErrorType:
    case WorkerType:
    case RetentionType:
      assert(!"Cannot convert that into a retention");
      break;
    default:
      assert(!"Tag_val(v_) <= LastValueType");
  }
  if (! ok)
    std::cerr << "Cannot convert " << s.toStdString() << " into a value" << std::endl;
  return ret;
}

Bool::Bool(bool b_) : Value(BoolType), b(b_) {}

Bool::Bool() : Bool(false) {}

Bool::~Bool() {}

QString Bool::toQString() const
{
  if (b)
    return QCoreApplication::translate("QMainWindow", "true");
  else
    return QCoreApplication::translate("QMainWindow", "false");
}

value Bool::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, BoolType);
  Store_field(ret, 0, Val_bool(b));
  CAMLreturn(ret);
}

bool Bool::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Bool const &o = static_cast<Bool const &>(other);
  return b == o.b;
}

Int::Int(int64_t i_) : Value(IntType), i(i_) {}

Int::Int() : Int(0) {}

Int::~Int() {}

QString Int::toQString() const
{
  return QString::number(i);
}

value Int::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, IntType);
  Store_field(ret, 0, caml_copy_int64(i));
  CAMLreturn(ret);
}

bool Int::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Int const &o = static_cast<Int const &>(other);
  return i == o.i;
}

Float::Float(double d_) : Value(FloatType), d(d_) {}

Float::Float() : Float(0.) {}

Float::~Float() {}

QString Float::toQString() const
{
  return QString::number(d);
}

value Float::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, FloatType);
  Store_field(ret, 0, caml_copy_double(d));
  CAMLreturn(ret);
}

bool Float::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Float const &o = static_cast<Float const &>(other);
  return d == o.d;
}

String::String(QString s_) : Value(StringType), s(s_) {}

String::String() : String("") {}

String::~String() {}

QString String::toQString() const
{
  return s;
}

value String::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, StringType);
  Store_field(ret, 0, caml_copy_string(s.toStdString().c_str()));
  CAMLreturn(ret);
}

bool String::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  String const &o = static_cast<String const &>(other);
  return s == o.s;
}

Error::Error(double time_, unsigned cmdId_, std::string const &msg_) :
  Value(ErrorType), time(time_), cmdId(cmdId_), msg(msg_) {}

Error::Error() : Error(0., 0, "") {}

Error::~Error() {}

QString Error::toQString() const
{
  (void)time;
  return QString::fromStdString(msg); // TODO: prepend with time etc.
}

value Error::toOCamlValue() const
{
  assert(!"Don't know how to convert from an Error");
}

bool Error::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Error const &o = static_cast<Error const &>(other);
  return cmdId == o.cmdId;
}

Worker::Worker(QString const &site_, QString const &program_, QString const &function_) :
  Value(WorkerType), site(site_), program(program_), function(function_) {}

Worker::Worker() : Worker("", "", "") {}

Worker::~Worker() {}

QString Worker::toQString() const
{
  return site + "/" + program + "/" + function;
}

value Worker::toOCamlValue() const
{
  assert(!"Don't know how to convert from a Worker");
}

bool Worker::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Worker const &o = static_cast<Worker const &>(other);
  return site == o.site && program == o.program && function == o.function;
}

Retention::Retention(double duration_, double period_) :
  Value(RetentionType), duration(duration_), period(period_) {}

Retention::Retention() : Retention(0., 0.) {}

Retention::~Retention() {}

bool Retention::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Retention const &o = static_cast<Retention const &>(other);
  return duration == o.duration && period == o.period;
}

QString Retention::toQString() const
{
  return QString("duration: ").
         append(QString::number(duration)).
         append(", period: ").
         append(QString::number(period));
}

value Retention::toOCamlValue() const
{
  assert(!"Don't know how to convert form a Retention");
}

TimeRange::TimeRange(std::vector<std::pair<double, double>> const &range_) :
  Value(TimeRangeType), range(range_) {}

TimeRange::TimeRange() : TimeRange(std::vector<std::pair<double,double>>()) {}

TimeRange::~TimeRange() {}

QString TimeRange::toQString() const
{
  return QString("TODO: TimeRange to string");
}

value TimeRange::toOCamlValue() const
{
  assert(!"Don't know how to convert from a TimeRange");
}

bool TimeRange::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  TimeRange const &o = static_cast<TimeRange const &>(other);
  return range == o.range;
}

Tuple::Tuple(unsigned skipped_, unsigned char const *bytes_, size_t size_) :
  Value(TupleType), skipped(skipped_), size(size_)
{
  if (bytes_) {
    bytes = new char[size];
    memcpy((void *)bytes, (void *)bytes_, size);
  } else {
    assert(size == 0);
    bytes = nullptr;
  }
}

Tuple::Tuple() : Tuple(0, nullptr, 0) {}

Tuple::~Tuple()
{
  if (bytes) delete[](bytes);
}

QString Tuple::toQString() const
{
  return QString::number(size) + QString(" bytes");
}

ser::Value *Tuple::unserialize(std::shared_ptr<RamenType const> type) const
{
  uint32_t const *start = (uint32_t const *)bytes;  // TODO: check alignment
  uint32_t const *max = (uint32_t const *)(bytes + size); // TODO: idem
  ser::Value *v = ser::unserialize(type, start, max, true);
  assert(start == max);
  return v;
}

value Tuple::toOCamlValue() const
{
  assert(!"Don't know how to convert from an Tuple");
}

bool Tuple::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Tuple const &o = static_cast<Tuple const &>(other);
  return size == o.size && 0 == memcmp(bytes, o.bytes, size);
}

RamenType::RamenType(ser::ValueType type_, bool nullable_) :
  Value(RamenTypeType), type(type_), nullable(nullable_)
{}

RamenType::RamenType() : RamenType(ser::AnyType, false) {}

RamenType::~RamenType() {}

value RamenType::toOCamlValue() const
{
  assert(!"Don't know how to convert from a RamenType");
}

bool RamenType::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  RamenType const &o = static_cast<RamenType const &>(other);
  return type == o.type;
}

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

bool RamenValueValue::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  RamenValueValue const &o = static_cast<RamenValueValue const &>(other);
  return value == o.value;
}

SourceInfo::SourceInfo() : SourceInfo(QString(), QString()) {}

SourceInfo::~SourceInfo()
{
  while (! params.isEmpty()) {
    delete (params.takeLast());
  }
  while (! infos.isEmpty()) {
    delete (infos.takeLast());
  }
}

bool SourceInfo::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  SourceInfo const &o = static_cast<SourceInfo const &>(other);
  if (md5 != o.md5) return false;
  if (isInfo()) {
    return o.isInfo() && params == o.params && infos == o.infos;
  } else {
    return !o.isInfo() && errMsg == o.errMsg;
  }
}


std::ostream &operator<<(std::ostream &os, Value const &v)
{
  os << v.toQString().toStdString();
  return os;
}

};
