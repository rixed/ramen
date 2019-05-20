#ifndef CONFVALUE_H_190504
#define CONFVALUE_H_190504
#include <iostream>
#include <QCoreApplication>
#include <QString>
#include <QMetaType>
extern "C" {
# include <caml/mlvalues.h>
}
#include "serValue.h"

namespace conf {

enum ValueType {
  BoolType = 0, IntType, FloatType, StringType,
  ErrorType, WorkerType, RetentionType, TimeRangeType,
  // Beware: conf::TupleType is the type of a tuple received for tailling
  // (ie number of skipped tuples + serialized value), while
  // ser::TupleType is the type of a RamenValue (equivalent to T.TTuple).
  TupleType, RamenTypeType,
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

struct Bool : public Value
{
  bool b;
  Bool();
  ~Bool();
  Bool(bool);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Int : public Value
{
  int64_t i;
  Int();
  ~Int();
  Int(int64_t);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Float : public Value
{
  double d;
  Float();
  ~Float();
  Float(double);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct String : public Value
{
  QString s;
  String();
  ~String();
  String(QString);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Error : public Value
{
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

struct Worker : public Value
{
  QString site;
  QString program;
  QString function;
  Worker();
  ~Worker();
  Worker(QString const &, QString const &, QString const &);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Retention : public Value
{
  double duration;
  double period;
  Retention();
  ~Retention();
  Retention(double, double);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct TimeRange : public Value
{
  std::vector<std::pair<double, double>> range;
  TimeRange();
  ~TimeRange();
  TimeRange(std::vector<std::pair<double, double>> const &);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct Tuple : public Value
{
  unsigned skipped;
  char const *bytes;
  size_t size;
  Tuple();
  ~Tuple();
  Tuple(unsigned, unsigned char const *, size_t);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct RamenType : public Value
{
  ser::ValueType type;
  bool nullable;
  RamenType();
  ~RamenType();
  RamenType(ser::ValueType, bool);
  QString toQString() const;
  value toOCamlValue() const;
  virtual bool operator==(Value const &) const;
  virtual unsigned numColumns() const { return 0; };
  virtual QString header(size_t) const { return QString(); };
};

struct RamenTypeScalar : public RamenType
{
  RamenTypeScalar(ser::ValueType t, bool n) : RamenType(t, n) {}
  unsigned numColumns() const { return 1; }
  QString header(size_t) const;
};

struct RamenTypeTuple : public RamenType
{
  // For Tuples and other composed types, subTypes are owned by their parent
  std::vector<RamenType *> fields;
  RamenTypeTuple(std::vector<RamenType *> fields_, bool n) :
    RamenType(ser::TupleType, n), fields(fields_) {}
  // Maybe displaying a tuple in a single column would be preferable?
  unsigned numColumns() const { return fields.size(); }
  QString header(size_t i) const
  {
    if (i >= fields.size()) return QString();
    return QString("#") + QString::number(i);
  };
};

struct RamenTypeVec : public RamenType
{
  unsigned dim;
  RamenType *subType;
  RamenTypeVec(unsigned dim_, RamenType *subType_, bool n) :
    RamenType(ser::VecType, n), dim(dim_), subType(subType_) {}
  // Maybe displaying a vector in a single column would be preferable?
  unsigned numColumns() const { return dim; }
  QString header(size_t i) const
  {
    if (i >= dim) return QString();
    return QString("#") + QString::number(i);
  };
};

struct RamenTypeList : public RamenType
{
  RamenType *subType;
  RamenTypeList(RamenType *subType_, bool n) :
    RamenType(ser::ListType, n), subType(subType_) {}
  // Lists are displayed in a single column as they have a variable length
  unsigned numColumns() const { return 1; }
  QString header(size_t i) const
  {
    if (i != 0) return QString();
    return QString(QCoreApplication::translate("QMainWindow", "list"));
  };
};

// This is the interesting one:
struct RamenTypeRecord : public RamenType
{
  std::vector<std::pair<QString, RamenType *>> fields;
  RamenTypeRecord(std::vector<std::pair<QString, RamenType *>> fields_, bool n) :
    RamenType(ser::RecordType, n), fields(fields_) {}
  unsigned numColumns() const { return fields.size(); }
  QString header(size_t i) const
  {
    if (i >= fields.size()) return QString();
    return fields[i].first;
  };
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
Q_DECLARE_METATYPE(conf::Tuple);

// Defined by OCaml header but conflicting with further Qt includes:
#undef alloc

#endif
