#ifndef CONFVALUE_H_190504
#define CONFVALUE_H_190504
#include <iostream>
#include <QString>
#include <QMetaType>
extern "C" {
# include <caml/mlvalues.h>
}

namespace conf {

enum ValueType {
  BoolType = 0, IntType, FloatType, StringType,
  ErrorType, WorkerType, RetentionType, TimeRangeType,
  LastValueType
};

class Value
{
  ValueType valueType;
public:
  // Construct uninitialized
  Value();
  Value(ValueType);
  virtual ~Value();

  virtual QString toQString() const;
  virtual value toOCamlValue() const;
  virtual bool operator==(Value const &) const;
  bool operator!=(Value const &) const;
};

std::ostream &operator<<(std::ostream &, Value const &);

// Construct from an OCaml value
Value *valueOfOCaml(value);
// Construct from a QString
Value *valueOfQString(conf::ValueType, QString const &);

struct Bool : public Value {
  bool b;
  Bool();
  ~Bool();
  Bool(bool);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Int : public Value {
  int64_t i;
  Int();
  ~Int();
  Int(int64_t);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Float : public Value {
  double d;
  Float();
  ~Float();
  Float(double);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct String : public Value {
  QString s;
  String();
  ~String();
  String(QString);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Error : public Value {
  double time;
  unsigned cmd_id;
  std::string msg;
  Error();
  ~Error();
  Error(double, unsigned, std::string const &);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Worker : public Value {
  std::string site;
  std::string program;
  std::string function;
  Worker();
  ~Worker();
  Worker(std::string const &, std::string const &, std::string const &);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct TimeRange : public Value {
  std::vector<std::pair<double, double>> range;
  TimeRange();
  ~TimeRange();
  TimeRange(std::vector<std::pair<double, double>> const &);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Retention : public Value {
  double duration;
  double period;
  Retention();
  ~Retention();
  Retention(double, double);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

};

Q_DECLARE_METATYPE(std::shared_ptr<conf::Value const>);
Q_DECLARE_METATYPE(conf::Bool);
Q_DECLARE_METATYPE(conf::Int);
Q_DECLARE_METATYPE(conf::Float);
Q_DECLARE_METATYPE(conf::String);
Q_DECLARE_METATYPE(conf::Error);
Q_DECLARE_METATYPE(conf::Worker);
Q_DECLARE_METATYPE(conf::TimeRange);
Q_DECLARE_METATYPE(conf::Retention);

#endif
