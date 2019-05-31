#ifndef CONFVALUE_H_190504
#define CONFVALUE_H_190504
#include <iostream>
#include <cassert>
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
  unsigned cmdId;
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

struct RamenType;

/* Tuple of ramen _values_, not to be confused with RamenTypeTuple which is a tuple
 * of ramen _types_.
 * Actually, nothing mandates [val] to be a tuple; in the future, when I/O are not
 * restricted to tuples, rename this Tuple into just RamenValue? */
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
  // returned value belongs to caller:
  ser::Value *unserialize(std::shared_ptr<RamenType const>) const;
};

struct RamenType : public Value
{
  ser::ValueType type;
  bool nullable;
  RamenType();
  ~RamenType();
  RamenType(ser::ValueType, bool);
  virtual QString structureToQString() const = 0;
  QString toQString() const
  {
    QString s(structureToQString());
    if (nullable) s.append("?");
    return s;
  }
  value toOCamlValue() const;
  virtual bool operator==(Value const &) const;
  virtual unsigned numColumns() const { return 0; };
  virtual QString columnName(unsigned) const = 0;
  virtual std::shared_ptr<RamenType const> columnType(unsigned) const = 0;
  // Return the size of the required nullmask in _bits_
  // ie. number of nullable subfields.
  virtual size_t nullmaskWidth(bool) const { return 0; }
  virtual bool isNumeric() const = 0;
};

struct RamenTypeScalar : public RamenType
{
  RamenTypeScalar(ser::ValueType t, bool n) : RamenType(t, n) {}
  QString structureToQString() const;
  unsigned numColumns() const { return 1; }
  QString columnName(unsigned) const;
  std::shared_ptr<RamenType const> columnType(unsigned) const { return nullptr; }
  bool isNumeric() const;
};

struct RamenTypeTuple : public RamenType
{
  // For Tuples and other composed types, subTypes are owned by their parent
  std::vector<std::shared_ptr<RamenType const>> fields;
  RamenTypeTuple(std::vector<std::shared_ptr<RamenType const>> fields_, bool n) :
    RamenType(ser::TupleType, n), fields(fields_) {}
  QString structureToQString() const;
  // Maybe displaying a tuple in a single column would be preferable?
  unsigned numColumns() const { return fields.size(); }
  QString columnName(unsigned i) const
  {
    if (i >= fields.size()) return QString();
    return QString("#") + QString::number(i);
  }
  std::shared_ptr<RamenType const> columnType(unsigned i) const
  {
    if (i >= fields.size()) return nullptr;
    return fields[i];
  }
  size_t nullmaskWidth(bool) const { return fields.size(); }
  bool isNumeric() const { return false; }
};

struct RamenTypeVec : public RamenType
{
  unsigned dim;
  std::shared_ptr<RamenType const> subType;
  RamenTypeVec(unsigned dim_, std::shared_ptr<RamenType const> subType_, bool n) :
    RamenType(ser::VecType, n), dim(dim_), subType(subType_) {}
  QString structureToQString() const;
  // Maybe displaying a vector in a single column would be preferable?
  unsigned numColumns() const { return dim; }
  QString columnName(unsigned i) const
  {
    if (i >= dim) return QString();
    return QString("#") + QString::number(i);
  }
  std::shared_ptr<RamenType const> columnType(unsigned i) const
  {
    if (i >= dim) return nullptr;
    return subType;
  }
  size_t nullmaskWidth(bool) const { return dim; }
  bool isNumeric() const { return false; }
};

struct RamenTypeList : public RamenType
{
  std::shared_ptr<RamenType const> subType;
  RamenTypeList(std::shared_ptr<RamenType const> subType_, bool n) :
    RamenType(ser::ListType, n), subType(subType_) {}
  QString structureToQString() const;
  // Lists are displayed in a single column as they have a variable length
  unsigned numColumns() const { return 1; }
  QString columnName(unsigned i) const
  {
    if (i != 0) return QString();
    return QString(QCoreApplication::translate("QMainWindow", "list"));
  }
  std::shared_ptr<RamenType const> columnType(unsigned i) const
  {
    if (i != 0) return nullptr;
    return std::shared_ptr<RamenType const>(new RamenTypeList(subType, nullable));;
  }
  size_t nullmaskWidth(bool) const
  {
    assert(!"List nullmaskWidth is special!");
  }
  bool isNumeric() const { return false; }
};

// This is the interesting one:
struct RamenTypeRecord : public RamenType
{
  // Fields are ordered in serialization order:
  std::vector<std::pair<QString, std::shared_ptr<RamenType const>>> fields;
  // Here is the serialization order:
  std::vector<uint16_t> serOrder;

  RamenTypeRecord(std::vector<std::pair<QString, std::shared_ptr<RamenType const>>> fields_, bool n);
  QString structureToQString() const;
  unsigned numColumns() const { return fields.size(); }
  QString columnName(unsigned i) const
  {
    if (i >= fields.size()) return QString();
    return fields[i].first;
  }
  std::shared_ptr<RamenType const> columnType(unsigned i) const
  {
    if (i >= fields.size()) return nullptr;
    return fields[i].second;
  }
  size_t nullmaskWidth(bool topLevel) const
  {
    // TODO: fix nullmask for compound types (ie: reserve a bit only to
    //       nullable subfields)
    if (topLevel) {
      size_t w = 0;
      for (auto &f : fields) {
        if (f.second->nullable) w++;
      }
      return w;
    } else {
      return fields.size();
    }
  }
  bool isNumeric() const { return false; }
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

// Defined by OCaml columnName but conflicting with further Qt includes:
#undef alloc

#endif
