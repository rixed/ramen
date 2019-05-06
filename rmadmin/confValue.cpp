#include <string.h>
#include <cstdlib>
#include <QtWidgets>
#include <QString>
#include "confValue.h"

namespace conf {

Value::Value()
{
  valueType = LastValueType;
}

bool Value::is_initialized() const
{
  return (valueType < LastValueType);
}

Value::Value(value v_)
{
  assert(Is_block(v_));
  valueType = (ValueType)Tag_val(v_);
  switch (valueType) {
    case Bool:
      v.Bool = Bool_val(Field(v_, 0));
      break;
    case Int:
      v.Int = Int64_val(Field(v_, 0));
      break;
    case Float:
      v.Float = Double_val(Field(v_, 0));
      break;
    case Time:
      v.Time = Double_val(Field(v_, 0));
      break;
    case String:
      v.String = String_val(Field(v_, 0));
      break;
    case Error:
      v.Error = {
        .time = Double_val(Field(v_, 0)),
        .cmd_id = (unsigned)Int_val(Field(v_, 1)),
        .msg = strdup(String_val(Field(v_, 2)))
      };
      break;
    case Retention:
      v.Retention = {
        .duration = Double_val(Field(v_, 0)),
        .period = Double_val(Field(v_, 1))
      };
      break;
    case Dataset:
      v.Dataset = { // TODO
        .capa = 0, .length = 0, .next = 0, .arr = nullptr
      };
      break;
    case LastValueType:
      assert(!"Tag_val(v_) <= LastValueType");
  }
}

Value::Value(const Value& other)
{
  valueType = other.valueType;
  switch (valueType) {
    case Bool:
      v.Bool = other.v.Bool;
      break;
    case Int:
      v.Int = other.v.Int;
      break;
    case Float:
      v.Float = other.v.Float;
      break;
    case Time:
      v.Time = other.v.Time;
      break;
    case String:
      v.String = other.v.String;
      break;
    case Error:
      v.Error = other.v.Error;
      v.Error.msg = strdup(other.v.Error.msg);
      break;
    case Retention:
      v.Retention = other.v.Retention;
      break;
    case Dataset:
      v.Dataset = other.v.Dataset;
      break;
    case LastValueType:
      break;
  }
}

Value::Value(ValueType vt, QString const &s)
{
  (void)vt; (void)s;
  Value(); // TODO
}

Value::~Value()
{
  if (valueType == Error) {
    free(v.Error.msg);
  }
}

Value& Value::operator=(const Value& other)
{
  valueType = other.valueType;
  switch (valueType) {
    case Bool:
      v.Bool = other.v.Bool;
      break;
    case Int:
      v.Int = other.v.Int;
      break;
    case Float:
      v.Float = other.v.Float;
      break;
    case Time:
      v.Time = other.v.Time;
      break;
    case String:
      v.String = other.v.String;
      break;
    case Error:
      v.Error = other.v.Error;
      v.Error.msg = strdup(other.v.Error.msg);
      break;
    case Retention:
      v.Retention = other.v.Retention;
      break;
    case Dataset:
      v.Dataset = other.v.Dataset;
      break;
    case LastValueType:
      break;
  }
  return *this;
}

QString Value::toQString() const
{
  switch (valueType) {
    case Bool:
      if (v.Bool)
          return QCoreApplication::translate("QMainWindow", "true");
      else
          return QCoreApplication::translate("QMainWindow", "false");
    case Int:
      return QString::number(v.Int);
    case Float:
      return QString::number(v.Float);
    case Time:
      return QString::number(v.Time);  // TODO: convert to date+time
    case String:
      return v.String;
    case Error:
      return QString(v.Error.msg); // TODO: prepend with time etc.
    case Retention:
      return QString("duration: ").
             append(QString::number(v.Retention.duration)).
             append(", period: ").
             append(QString::number(v.Retention.period));
    case Dataset:
      return QString("TODO: string of dataset");
    case LastValueType:
      return QString("");
  }
}

bool operator==(Value const &a, Value const &b)
{
  if (a.valueType != b.valueType) return false;
  switch (a.valueType) {
    case Bool:
      return a.v.Bool == b.v.Bool;
    case Int:
      return a.v.Int == b.v.Int;
    case Float:
      return a.v.Float == b.v.Float;
    case Time:
      return a.v.Time == b.v.Time;
    case String:
      return a.v.String == b.v.String;
    case Error:
      return a.v.Error.cmd_id == b.v.Error.cmd_id;
    case Retention:
      return a.v.Retention == b.v.Retention;
    case Dataset:
      return a.v.Dataset == b.v.Dataset;
    case LastValueType:
      return true;
 }
}

bool operator!=(Value const &a, Value const &b)
{
  return !(a == b);
}

bool operator==(struct Error const &a, struct Error const &b)
{
  return a.cmd_id == b.cmd_id;  // Same as in OCaml
}

bool operator!=(struct Error const &a, struct Error const &b)
{
  return !(a == b);
}

bool operator==(struct Retention const &a, struct Retention const &b)
{
  return a.duration == b.duration &&
         a.period == b.period;
}

bool operator!=(struct Retention const &a, struct Retention const &b)
{
  return !(a == b);
}

bool operator==(struct Dataset const &a, struct Dataset const &b)
{
  (void)a; (void)b;
  return false; // TODO
}

bool operator!=(struct Dataset const &a, struct Dataset const &b)
{
  return !(a == b);
}

std::ostream &operator<<(std::ostream &os, Value const &v)
{
  os << v.toQString().toStdString();
  return os;
}

};
