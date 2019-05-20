#ifndef SERVALUE_H_190516
#define SERVALUE_H_190516
#include <QString>

/* Allow to manipulate (for now, just turn into a printable string)
 * serialized values (from a received Tuple but one day also from the
 * ringbuffers).
 *
 * the idea is of course that the actual bytes are not copied but stays in
 * the ringbuffer.
 */

namespace ser {

enum ValueType {
  Error = 0,    // RamenTypes has a "empty" there
  FloatType,
  StringType,
  BoolType,
  // RamenTypes has TNum and TAny there
  U8Type = 6, U16Type, U32Type, U64Type, U128Type,
  I8Type, I16Type, I32Type, I64Type, I128Type,
  EthType, Ipv4Type, Ipv6Type, IpType,
  Cidrv4Type, Cidrv6Type, CidrType,
  TupleType, VecType, ListType, RecordType
};

class Value {
  enum ValueType type;
  QString errMsg; // in case of unserialization error

public:
  uint32_t const *start, *stop; // first and past end words of the value
  Value(ValueType, uint32_t const *, uint32_t const *, uint32_t const *max);
  Value(QString const &);
  virtual ~Value();

  virtual QString toQString() const;
  virtual bool operator==(Value const &) const;
  bool operator!=(Value const &) const;

  size_t wordSize() const;
  size_t byteSize() const;
};

std::ostream &operator<<(std::ostream &, Value const &);

// Construct from a type and bytes
Value *unserialize(ValueType, uint32_t const *, uint32_t const *max);

// TODO: specialize

};

#endif
