#ifndef RAMENVALUE_H_190603
#define RAMENVALUE_H_190603
#include <cassert>
#include <memory>
#include <optional>
#include <thread>
#include <typeinfo>
#include <vector>
#include <QString>
#include <QWidget>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "misc.h"
#include "DessserValueType.h"

/*
 * A RamenType is a value-type + the nullability flag.
 * Function input and output have types, compound types have subtypes.
 * Values have only a value-type but no RamenType.
 * It is not possible to get the RamenType of a value as we do not know, unless
 * it's VNull, if it's nullable (and if it is VNull, then we do not know its
 * value-type).
 * It is not even possible to retrieve the value-type of a value, because
 * of subfields (it is possible to retrieve the value-type of scalar values
 * though).
 * But it is possible to build a possible type for any value (as
 * RamenTypes.type_of_value does). This is all we really need.
 */

class AtomicWidget;

// This class must stay abstract
struct RamenValue {
  virtual ~RamenValue() {}

  // FIXME: take a pointer for the optional key
  virtual QString const toQString(std::string const &) const = 0;
  virtual value toOCamlValue() const {
    assert(!"Unimplemented RamenValue::toOCamlValue");
  }

  // Tells if the value is Null:
  virtual bool isNull() const { return false; }

  // Used by conf::RamenValueValue.operator==:
  virtual bool operator==(RamenValue const &that) const {
    return typeid(*this).hash_code() == typeid(that).hash_code();
    // Then derived types must also compare the value!
  }

  bool operator!=(RamenValue const &that) const {
    return (! operator==(that));
  }

  /* Construct from an OCaml value of type RamenTypes.value
   * Returns the actual class for that value! */
  static RamenValue *ofOCaml(value);

  // Used for plotting
  virtual std::optional<double> toDouble() const { return std::optional<double>(); }
  virtual RamenValue const *columnValue(size_t c) const {
    assert(0 == c);
    return this;
  }

  /* Some keys have additional constraints or specific representations
   * more suitable than the generic editor for that value type.
   * But this is true for other methods of the Value. Let's rather
   * consider that Value can have "styles" depending on their key, which
   * allow them to customize their editor and/or other members. */
  virtual AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

QDebug operator<<(QDebug debug, RamenValue const &);

struct VNull : public RamenValue {
  QString const toQString(std::string const &) const {
    return QString("NULL");
  }
  value toOCamlValue() const;
  bool isNull() const { return true; }
};

struct VFloat : public RamenValue {
  double v;

  VFloat(double v_) : v(v_) {}
  VFloat() : VFloat(0) {}

  QString const toQString(std::string const &) const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return v; }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VString : public RamenValue {
  QString const v;

  VString(QString const v_) : v(v_) {}
  VString() : VString(QString()) {}

  QString const toQString(std::string const &) const { return v; }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VBool : public RamenValue {
  bool v;

  VBool(bool v_) : v(v_) {}
  VBool() : VBool(false) {}

  QString const toQString(std::string const &) const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VChar : public RamenValue {
  char v;

  VChar(char v_) : v(v_) {}
  VChar() : VChar(0) {}

  QString const toQString(std::string const &) const;
  value toOCamlValue() const;
  bool operator==(RamenValue const&) const;
  virtual std::optional<double> toDouble() const {return (double)v; }
  static VChar *ofQString(QString const &s);
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VU8 : public RamenValue {
  uint8_t v;

  VU8(uint8_t v_) : v(v_) {}
  VU8() : VU8(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VU8 *ofQString(QString const &s) { return new VU8(s.toInt()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VU16 : public RamenValue {
  uint16_t v;

  VU16(uint16_t v_) : v(v_) {}
  VU16() : VU16(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VU16 *ofQString(QString const &s) { return new VU16(s.toInt()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VU24 : public RamenValue {
  uint32_t v;

  VU24(uint32_t v_) : v(v_) {}
  VU24() : VU24(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VU24 *ofQString(QString const &s) { return new VU24(s.toInt()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VU32 : public RamenValue {
  uint32_t v;

  VU32(uint32_t v_) : v(v_) {}
  VU32() : VU32(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VU32 *ofQString(QString const &s) { return new VU32(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VU40 : public RamenValue {
  uint64_t v;

  VU40(uint64_t v_) : v(v_) {}
  VU40() : VU40(0) {}

  // TODO: if the key name ends with "_size" then use stringOfSize
  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VU40 *ofQString(QString const &s) { return new VU40(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VU48 : public RamenValue {
  uint64_t v;

  VU48(uint64_t v_) : v(v_) {}
  VU48() : VU48(0) {}

  // TODO: if the key name ends with "_size" then use stringOfSize
  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VU48 *ofQString(QString const &s) { return new VU48(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VU56 : public RamenValue {
  uint64_t v;

  VU56(uint64_t v_) : v(v_) {}
  VU56() : VU56(0) {}

  // TODO: if the key name ends with "_size" then use stringOfSize
  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VU56 *ofQString(QString const &s) { return new VU56(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VU64 : public RamenValue {
  uint64_t v;

  VU64(uint64_t v_) : v(v_) {}
  VU64() : VU64(0) {}

  // TODO: if the key name ends with "_size" then use stringOfSize
  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VU64 *ofQString(QString const &s) { return new VU64(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VU128 : public RamenValue {
  uint128_t v;

  VU128(uint128_t v_) : v(v_) {}
  VU128() : VU128(0) {}

  // TODO: if the key name ends with "_size" then use stringOfSize
  QString const toQString(std::string const &) const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  // FIXME:
  static VU128 *ofQString(QString const &s) { return new VU128(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VI8 : public RamenValue {
  int8_t v;

  VI8(int8_t v_) : v(v_) {}
  VI8() : VI8(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VI8 *ofQString(QString const &s) { return new VI8(s.toInt()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VI16 : public RamenValue {
  int16_t v;

  VI16(int16_t v_) : v(v_) {}
  VI16() : VI16(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VI16 *ofQString(QString const &s) { return new VI16(s.toInt()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VI24 : public RamenValue {
  int32_t v;

  VI24(int32_t v_) : v(v_) {}
  VI24() : VI24(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VI24 *ofQString(QString const &s) { return new VI24(s.toInt()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VI32 : public RamenValue {
  int32_t v;

  VI32(int32_t v_) : v(v_) {}
  VI32() : VI32(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VI32 *ofQString(QString const &s) { return new VI32(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VI40 : public RamenValue {
  int64_t v;

  VI40(int64_t v_) : v(v_) {}
  VI40() : VI40(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VI40 *ofQString(QString const &s) { return new VI40(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VI48 : public RamenValue {
  int64_t v;

  VI48(int64_t v_) : v(v_) {}
  VI48() : VI48(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VI48 *ofQString(QString const &s) { return new VI48(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VI56 : public RamenValue {
  int64_t v;

  VI56(int64_t v_) : v(v_) {}
  VI56() : VI56(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VI56 *ofQString(QString const &s) { return new VI56(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VI64 : public RamenValue {
  int64_t v;

  VI64(int64_t v_) : v(v_) {}
  VI64() : VI64(0) {}

  QString const toQString(std::string const &) const {
    return QString::number(v);
  }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  static VI64 *ofQString(QString const &s) { return new VI64(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VI128 : public RamenValue {
  int128_t v;

  VI128(int128_t v_) : v(v_) {}
  VI128() : VI128(0) {}

  QString const toQString(std::string const &) const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
  // FIXME:
  static VI128 *ofQString(QString const &s) { return new VI128(s.toLongLong()); }
  AtomicWidget *editorWidget(std::string const &, QWidget *parent = nullptr) const;
};

struct VEth : public RamenValue {
  uint64_t v;

  VEth(uint64_t v_) : v(v_) {}
  VEth() : VEth(0) {}

  QString const toQString(std::string const &) const;
};

struct VIpv4 : public RamenValue {
  uint32_t v;

  VIpv4(uint32_t v_) : v(v_) {}
  VIpv4() : VIpv4(0) {}

  QString const toQString(std::string const &) const;
};

struct VIpv6 : public RamenValue {
  uint128_t v;

  VIpv6(uint128_t v_) : v(v_) {}
  VIpv6() : VIpv6(0) {}

  QString const toQString(std::string const &) const;
};

struct VIp : public RamenValue {
  uint128_t v;
  bool isV4;

  VIp(uint128_t v_) : v(v_), isV4(false) {}
  VIp(uint32_t v_) : v(v_), isV4(true) {}
  VIp() : VIp((uint32_t)0) {}

  QString const toQString(std::string const &) const;
};

struct VCidrv4 : public RamenValue {
  VIpv4 ip;
  uint8_t mask;

  VCidrv4(uint32_t ip_, uint8_t mask_) : ip(ip_), mask(mask_) {}
  VCidrv4() : VCidrv4(0, 0) {}

  QString const toQString(std::string const &) const;
};

struct VCidrv6 : public RamenValue {
  VIpv6 ip;
  uint8_t mask;

  VCidrv6(uint128_t ip_, uint8_t mask_) : ip(ip_), mask(mask_) {}
  VCidrv6() : VCidrv6(0, 0) {}

  QString const toQString(std::string const &) const;
};

struct VCidr : public RamenValue {
  VIp ip;
  uint8_t mask;

  VCidr(uint128_t ip_, uint8_t mask_) : ip(ip_), mask(mask_) {}
  VCidr(uint32_t ip_, uint8_t mask_) : ip(ip_), mask(mask_) {}
  VCidr() : VCidr((uint32_t)0, 0) {}

  QString const toQString(std::string const &) const;
};

struct VTuple : public RamenValue {
  std::vector<RamenValue const *> v;

  VTuple(size_t numFields) { v.reserve(numFields); }
  VTuple(value);

  QString const toQString(std::string const &) const;
  void append(RamenValue const *);
  virtual RamenValue const *columnValue(size_t c) const {
    if (c >= v.size()) return nullptr;
    return v[c];
  }
};

struct VVec : public RamenValue {
  std::vector<RamenValue const *> v;

  VVec(size_t dim) { v.reserve(dim); }
  VVec(value);

  QString const toQString(std::string const &) const;
  void append(RamenValue const *i) {
    assert(v.size() < v.capacity());
    v.push_back(i);
  }
  virtual RamenValue const *columnValue(size_t c) const {
    if (c >= v.size()) return nullptr;
    return v[c];
  }
};

struct VLst : public RamenValue {
  std::vector<RamenValue const *> v;

  VLst(size_t dim) { v.reserve(dim); }
  VLst(value);

  QString const toQString(std::string const &) const;
  void append(RamenValue const *i) { v.push_back(i); }
};

struct VRecord : public RamenValue {
  std::vector<std::pair<QString, RamenValue const *>> v;

  /* VRecord fields are unserialized in another order so we built it
   * with a setter instead of an appender: */
  VRecord(size_t numFields);
  VRecord(value);

  QString const toQString(std::string const &) const;

  void set(size_t idx, QString const field, RamenValue const *);

  virtual RamenValue const *columnValue(size_t c) const {
    if (c >= v.size()) return nullptr;
    return v[c].second;
  }
};

// There should be no values of type Sum (yet) but for completeness:
struct VSum : public RamenValue {
  size_t label;
  QString cstrName;
  RamenValue const *v;

  VSum(size_t label, QString const &cstrName, RamenValue const *v);
  VSum(value);

  QString const toQString(std::string const &) const;
};



/* Help check toOcamlValue is always called from the OCaml thread: */

extern std::thread::id ocamlThreadId;

extern inline void checkInOCamlThread()
{
  assert(std::this_thread::get_id() == ocamlThreadId);
}

#endif
