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

struct RamenTypeStructure
{
  virtual ~RamenTypeStructure() {};
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
  /* Return the size of the required nullmask in _bits_, assuming the structure
   * is nullable. */
  virtual size_t nullmaskWidth(bool) const { return 1; }
  virtual bool isNumeric() const { return false; }
  // Some structure can build a RamenValue from a QString:
  virtual RamenValue *valueOfQString(QString const) const { return nullptr; }
  // All structures can unserialize a value:
  virtual RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const = 0;

  static RamenTypeStructure *ofOCaml(value);
};

struct TEmpty : RamenTypeStructure {  // Used for errors
  QString const toQString() const { return "Empty"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TFloat : RamenTypeStructure {  // Use OCaml tag values for non-block structures
  QString const toQString() const { return "Float"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TString : RamenTypeStructure {
  QString const toQString() const { return "String"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TBool : RamenTypeStructure {
  QString const toQString() const { return "Bool"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TNum : RamenTypeStructure {  // Not supposed to encounter that one here
  QString const toQString() const { return "Num"; }
  bool isNumeric() const { return true; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TAny : RamenTypeStructure {  // Makes sense to use this for NULL values
  QString const toQString() const { return "Any"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU8 : RamenTypeStructure {
  QString const toQString() const { return "U8"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU16 : RamenTypeStructure {
  QString const toQString() const { return "U16"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU32 : RamenTypeStructure {
  QString const toQString() const { return "U32"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU64 : RamenTypeStructure {
  QString const toQString() const { return "U64"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TU128 : RamenTypeStructure {
  QString const toQString() const { return "U128"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI8 : RamenTypeStructure {
  QString const toQString() const { return "I8"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI16 : RamenTypeStructure {
  QString const toQString() const { return "I16"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI32 : RamenTypeStructure {
  QString const toQString() const { return "I32"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI64 : RamenTypeStructure {
  QString const toQString() const { return "I64"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TI128 : RamenTypeStructure {
  QString const toQString() const { return "I128"; }
  bool isNumeric() const { return true; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TEth : RamenTypeStructure {
  QString const toQString() const { return "Eth"; }
  RamenValue *valueOfQString(QString const) const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TIpv4 : RamenTypeStructure {
  QString const toQString() const { return "Ipv4"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TIpv6 : RamenTypeStructure {
  QString const toQString() const { return "Ipv6"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TIp : RamenTypeStructure {
  QString const toQString() const { return "Ip"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TCidrv4 : RamenTypeStructure {
  QString const toQString() const { return "Cidrv4"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TCidrv6 : RamenTypeStructure {
  QString const toQString() const { return "Cidrv6"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TCidr : RamenTypeStructure {
  QString const toQString() const { return "Cidr"; }
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
};

struct TTuple : RamenTypeStructure {
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

struct TVec : RamenTypeStructure {
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

struct TList : RamenTypeStructure {
  std::shared_ptr<RamenType const> subType;

  TList(std::shared_ptr<RamenType const> subType_) : subType(subType_) {}

  bool isScalar() const { return false; }
  QString const toQString() const;
  RamenValue *unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const;
  size_t nullmaskWidth(bool) const;
};

struct TRecord : RamenTypeStructure {
  // Fields are ordered in user order:
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

std::ostream &operator<<(std::ostream &, RamenTypeStructure const &);

#endif
