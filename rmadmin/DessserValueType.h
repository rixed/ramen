#ifndef RAMENTYPESTRUCTURE_H_190716
#define RAMENTYPESTRUCTURE_H_190716
#include <memory>
#include <utility>
#include <vector>
#include <QString>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}

struct RamenType;
struct RamenValue;

struct DessserValueType
{
  virtual ~DessserValueType() {};

  virtual bool isScalar() const { return true; }
  operator QString() const
  {
    return toQString();
  }
  // TODO: remove and use the above cast operator:
  virtual QString const toQString() const = 0;
  virtual value toOCamlValue() const;
  virtual int numColumns() const { return 1; };
  virtual QString columnName(int) const { return QString(); }
  /* As one object cannot return itself as shared, this returns nullptr
   * when there are no subtypes: */
  virtual std::shared_ptr<RamenType const> columnType(int) const { return nullptr; }
  /* Return the size of the required nullmask in _bits_, assuming the type
   * is nullable. */
  virtual size_t nullmaskWidth(bool) const { return 1; }
  virtual bool isNumeric() const { return false; }
  // Some vtyps can build a RamenValue from a QString:
  virtual RamenValue *valueOfQString(QString const) const { return nullptr; }
  // Every value-type (but unknown) can unserialize a value:
  virtual RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const = 0;

  static DessserValueType *ofOCaml(value);
};

/*
 * Unknown, that we are never supposed to met:
 */

struct Unknown : DessserValueType {
  QString const toQString() const { return "Unknown"; }
  virtual bool isScalar() const { return false; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

/*
 * Machine types:
 */

struct DessserMacType : DessserValueType {
  virtual ~DessserMacType() {};
  virtual bool isNumeric() const { return true; }
};

struct TFloat : DessserMacType {
  QString const toQString() const { return "Float"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TString : DessserMacType {
  QString const toQString() const { return "String"; }
  RamenValue *valueOfQString(QString const) const;
  bool isNumeric() const { return false; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TBool : DessserMacType {
  QString const toQString() const { return "Bool"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TChar : DessserMacType {
  QString const toQString() const { return "Char"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU8 : DessserMacType {
  QString const toQString() const { return "U8"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU16 : DessserMacType {
  QString const toQString() const { return "U16"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU24 : DessserMacType {
  QString const toQString() const { return "U24"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU32 : DessserMacType {
  QString const toQString() const { return "U32"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU40 : DessserMacType {
  QString const toQString() const { return "U40"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU48 : DessserMacType {
  QString const toQString() const { return "U48"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU56 : DessserMacType {
  QString const toQString() const { return "U56"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU64 : DessserMacType {
  QString const toQString() const { return "U64"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU128 : DessserMacType {
  QString const toQString() const { return "U128"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI8 : DessserMacType {
  QString const toQString() const { return "I8"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI16 : DessserMacType {
  QString const toQString() const { return "I16"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI24 : DessserMacType {
  QString const toQString() const { return "I24"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI32 : DessserMacType {
  QString const toQString() const { return "I32"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI40 : DessserMacType {
  QString const toQString() const { return "I40"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI48 : DessserMacType {
  QString const toQString() const { return "I48"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI56 : DessserMacType {
  QString const toQString() const { return "I56"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI64 : DessserMacType {
  QString const toQString() const { return "I64"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI128 : DessserMacType {
  QString const toQString() const { return "I128"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

/*
 * Well known user types
 */

struct TEth : DessserValueType {
  QString const toQString() const { return "Eth"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TIpv4 : DessserValueType {
  QString const toQString() const { return "Ipv4"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TIpv6 : DessserValueType {
  QString const toQString() const { return "Ipv6"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TIp : DessserValueType {
  QString const toQString() const { return "Ip"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TCidrv4 : DessserValueType {
  QString const toQString() const { return "Cidrv4"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TCidrv6 : DessserValueType {
  QString const toQString() const { return "Cidrv6"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TCidr : DessserValueType {
  QString const toQString() const { return "Cidr"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

/*
 * Compound types
 */

struct TVec : DessserValueType {
  size_t dim;
  std::shared_ptr<RamenType const> subType;

  TVec(size_t dim_, std::shared_ptr<RamenType const> subType_) :
    dim(dim_), subType(subType_) {}

  bool isScalar() const { return false; }
  QString const toQString() const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
  // Vectors are displayed in dim columns. Maybe a single one would be preferable?
  int numColumns() const { return dim; }
  QString columnName(int) const;
  std::shared_ptr<RamenType const> columnType(int i) const;
  size_t nullmaskWidth(bool) const { return dim; }
};

struct TList : DessserValueType {
  std::shared_ptr<RamenType const> subType;

  TList(std::shared_ptr<RamenType const> subType_) : subType(subType_) {}

  bool isScalar() const { return false; }
  QString const toQString() const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
  size_t nullmaskWidth(bool) const;
};

struct TTuple : DessserValueType {
  /* No recusrive classes in C++, so we use a pointer to RamenType.
   * Notice we can share this pointer with callers of columnType(). */
  std::vector<std::shared_ptr<RamenType const>> fields;

  TTuple(size_t numFields) { fields.reserve(numFields); }
  // Tuples are constructed empty:
  void append(std::shared_ptr<RamenType const> const t) { fields.push_back(t); }

  bool isScalar() const { return false; }
  QString const toQString() const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
  int numColumns() const { return fields.size(); }
  QString columnName(int) const;
  std::shared_ptr<RamenType const> columnType(int i) const;
  size_t nullmaskWidth(bool) const { return fields.size(); }
};

struct TRecord : DessserValueType {
  // Fields are ordered in definition order:
  std::vector<std::pair<QString const, std::shared_ptr<RamenType const>>> fields;
  // Here is the serialization order:
  std::vector<uint16_t> serOrder;

  TRecord(size_t numFields) { fields.reserve(numFields); }

  /* Records are constructed empty, fields must be pushed one by one (serOrder
   * will be updated after each call to append). */
  void append(QString const, std::shared_ptr<RamenType const> const);

  bool isScalar() const { return false; }
  QString const toQString() const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
  int numColumns() const { return fields.size(); }
  QString columnName(int i) const;
  std::shared_ptr<RamenType const> columnType(int i) const;
  size_t nullmaskWidth(bool) const;
};

struct TSum : DessserValueType {
  // Alternatives are ordered in definition order:
  std::vector<std::pair<QString const, std::shared_ptr<RamenType const>>> alternatives;

  TSum(size_t numCstrs) { alternatives.reserve(numCstrs); }

  /* Like records, sums are constructed empty; alternatives must be pushed one by one. */
  void append(QString const, std::shared_ptr<RamenType const> const);

  bool isScalar() const { return false; }
  QString const toQString() const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

/*
 * TMap is not a real type, it has no value.
 */

std::ostream &operator<<(std::ostream &, DessserValueType const &);

#endif
