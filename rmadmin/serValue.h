#ifndef SERVALUE_H_190516
#define SERVALUE_H_190516
#include <memory>
#include <vector>
#include <optional>
#include <QString>
#include "misc.h"

/* Allow to manipulate (for now, just turn into a printable string)
 * serialized values (from a received Tuple but one day also from the
 * ringbuffers).
 *
 * the idea is of course that the actual bytes are not copied but stays in
 * the ringbuffer.
 */

namespace conf {
  struct RamenType;
};

namespace ser {

enum ValueType {
  // Those constructors have no parameters, so they are integers:
  EmptyType = 0,    // That we use for errors
  FloatType,
  StringType,
  BoolType,
  // RamenTypes has TNum there
  AnyType = 5,    // Used for NULLs
  U8Type = 6, U16Type, U32Type, U64Type, U128Type,
  I8Type, I16Type, I32Type, I64Type, I128Type,
  EthType, Ipv4Type, Ipv6Type, IpType,
  Cidrv4Type, Cidrv6Type, CidrType,
  // Those constructors have parameters, so they are blocks which tag starts at 0:
  TupleType, VecType, ListType, RecordType
};

class Value {
  enum ValueType type;

public:
  Value(ValueType);
  virtual ~Value();
  virtual int numColumns() const;
  virtual Value const *columnValue(int) const;

  virtual QString toQString() const = 0;
  virtual std::optional<double> toDouble() const { return std::optional<double>(); }
  virtual bool operator==(Value const &) const;
  bool operator!=(Value const &) const;
};

class Null : public Value
{
public:
  Null();
  QString toQString() const;
};

class Error : public Value
{
public:
  QString errMsg;
  Error(QString const &);
  QString toQString() const { return errMsg; }
};

/* FIXME: Those specialized types copy the value out of the tuple but instead they
 *        should probably point into the tuple (zero-copy deser).
 */
class Float : public Value
{
public:
  double v;
  Float(uint32_t const *&);
  QString toQString() const;
  virtual std::optional<double> toDouble() const { return v; }
};

class Bool : public Value
{
public:
  bool v;
  Bool(uint32_t const *&);
  QString toQString() const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class String : public Value
{
public:
  QString v;
  String(uint32_t const *&, size_t len);
  QString toQString() const { return v; };
};

class U8 : public Value
{
public:
  uint_least8_t v;
  U8(uint32_t const *&);
  QString toQString() const { return QString::number(v); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class U16 : public Value
{
public:
  uint_least16_t v;
  U16(uint32_t const *&);
  QString toQString() const { return QString::number(v); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class U32 : public Value
{
public:
  uint_least32_t v;
  U32(uint32_t const *&);
  QString toQString() const { return QString::number(v); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class U64 : public Value
{
public:
  uint_least64_t v;
  U64(uint32_t const *&);
  QString toQString() const { return QString::number(v); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class U128 : public Value
{
public:
  uint128_t v;
  U128(uint32_t const *&);
  QString toQString() const { return QString("TODO"); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class I8 : public Value
{
public:
  int_least8_t v;
  I8(uint32_t const *&);
  QString toQString() const { return QString::number(v); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class I16 : public Value
{
public:
  int_least16_t v;
  I16(uint32_t const *&);
  QString toQString() const { return QString::number(v); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class I32 : public Value
{
public:
  int_least32_t v;
  I32(uint32_t const *&);
  QString toQString() const { return QString::number(v); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class I64 : public Value
{
public:
  int_least64_t v;
  I64(uint32_t const *&);
  QString toQString() const { return QString::number(v); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class I128 : public Value
{
public:
  int128_t v;
  I128(uint32_t const *&);
  QString toQString() const { return QString("TODO"); }
  virtual std::optional<double> toDouble() const { return (double)v; }
};

class Eth : public Value
{
public:
  uint64_t v;
  Eth(uint32_t const *&);
  QString toQString() const { return QString("TODO"); }
};

class Ipv4 : public Value
{
public:
  uint32_t v;
  Ipv4(uint32_t const *&);
  QString toQString() const { return QString("TODO"); }
};

class Ipv6 : public Value
{
public:
  uint128_t v;
  Ipv6(uint32_t const *&);
  QString toQString() const { return QString("TODO"); }
};

class Tuple : public Value
{
public:
  std::vector<Value const *> fieldValues;
  Tuple(std::vector<Value const *> const &);
  int numColumns() const { return fieldValues.size(); }
  Value const *columnValue(int c) const
  {
    if (c > numColumns()) return nullptr;
    return fieldValues[c];
  }
  QString toQString() const;
};

class Vec : public Value
{
public:
  std::vector<Value const *> values;
  Vec(std::vector<Value const *> const &);
  int numColumns() const { return values.size(); }
  Value const *columnValue(int c) const
  {
    if (c > numColumns()) return nullptr;
    return values[c];
  }
  QString toQString() const;
};

class List : public Value
{
public:
  std::vector<Value const *> values;
  List(std::vector<Value const *> const &);
  int numColumns() const { return values.size(); }
  Value const *columnValue(int c) const
  {
    if (c > numColumns()) return nullptr;
    return values[c];
  }
  QString toQString() const;
};

class Record : public Value
{
public:
  // fieldValues is in user order:
  std::vector<std::pair<QString, Value const *>> fieldValues;
  Record(std::vector<std::pair<QString, Value const *>> const &);
  int numColumns() const { return fieldValues.size(); }

  Value const *columnValue(int c) const
  {
    if (c > numColumns()) return nullptr;
    return fieldValues[c].second;
  }
  QString toQString() const;
};


std::ostream &operator<<(std::ostream &, Value const &);

// Construct from a type and serialized bytes
extern Value *unserialize(std::shared_ptr<conf::RamenType const>, uint32_t const *&, uint32_t const *max, bool topLevel=false);

};

#endif
