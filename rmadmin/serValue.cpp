#include <iostream>
#include <cstring>
#include "serValue.h"

namespace ser {

size_t Value::wordSize() const
{
  return stop - start;
}

size_t Value::byteSize() const
{
  return sizeof(uint32_t) * wordSize();
}

Value::Value(ValueType type_, uint32_t const *start_, uint32_t const *stop_, uint32_t const *max) :
  type(type_), start(start_), stop(stop_)
{
  assert(stop >= start);
  if (stop > max) {
    type = Error;
    errMsg = QString("Cannot deserialize: ends at offset " +
                     QString::number((long)(stop-start)) + " but max is: " +
                     QString::number((long)(max-start)));
  }
}

Value::Value(QString const &errMsg_) :
  type(Error), errMsg(errMsg_)
{
  start = stop = nullptr;
}

Value::~Value() {}

QString Value::toQString() const
{
  if (type == Error) return errMsg;
  else return QString::number(wordSize()) + QString(" words");
}

bool Value::operator==(Value const &other) const
{
  if (other.type != type || other.wordSize() != wordSize()) return false;
  return 0 == std::memcmp(start, other.start, byteSize());
}

bool Value::operator!=(Value const &other) const
{
  return !operator==(other);
}

std::ostream &operator<<(std::ostream &os, Value const &v)
{
  os << v.toQString().toStdString();
  return os;
}

static size_t roundUpWords(size_t sz)
{
  if (!(sz & 0x3)) return sz;
  return (sz | 0x3) + 1;
}

Value *unserialize(ValueType valueType, uint32_t const *start, uint32_t const *max)
{
  // TODO: specialize
  size_t ws;
  switch (valueType) {
    case FloatType:
      ws = 2;
      return new Value(valueType, start, start + 2, max);
    case StringType:
      {
        if (max < start + 1)
          return new Value(valueType, start, start + 1, max);
        size_t len = roundUpWords(*start);
        return new Value(valueType, start, start + 1 + len, max);
      }
    case BoolType:
    case U8Type:
    case U16Type:
    case U32Type:
    case I8Type:
    case I16Type:
    case I32Type:
    case Ipv4Type:
      return new Value(valueType, start, start + 1, max);
    case U64Type:
    case I64Type:
    case EthType:
      return new Value(valueType, start, start + 2, max);
    case U128Type:
    case I128Type:
    case Ipv6Type:
      return new Value(valueType, start, start + 4, max);
    default:
      return new Value("TODO");
  }
}

};
