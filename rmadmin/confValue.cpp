#include <string.h>
#include <cstdlib>
#include <QtWidgets>
#include <QString>
#include "confValue.h"
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
}

namespace conf {

Value::Value()
{
  valueType = LastValueType;
}

Value::Value(ValueType valueType_) : valueType(valueType_) {}

Value::~Value() {}

QString Value::toQString() const
{
  return QString("undefined value");
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

Value *valueOfOCaml(value v_)
{
  CAMLparam1(v_);
  CAMLlocal1(tmp_);
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
      tmp_ = Field(v_, 0);
      assert(Tag_val(tmp_) == Double_array_tag);
      ret = new Retention(
        Double_field(tmp_, 0),
        Double_field(tmp_, 1));
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

Bool::Bool() : Bool(true) {}

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

Error::Error(double time_, unsigned cmd_id_, std::string const &msg_) :
  Value(ErrorType), time(time_), cmd_id(cmd_id_), msg(msg_) {}

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
  return cmd_id == o.cmd_id;
}

Worker::Worker(std::string const &site_, std::string const &program_, std::string const &function_) :
  Value(WorkerType), site(site_), program(program_), function(function_) {}

Worker::Worker() : Worker("", "", "") {}

Worker::~Worker() {}

QString Worker::toQString() const
{
  return
    QString::fromStdString(site + "/" + program + "/" + function);
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

std::ostream &operator<<(std::ostream &os, Value const &v)
{
  os << v.toQString().toStdString();
  return os;
}

};
