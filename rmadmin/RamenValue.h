#ifndef RAMENVALUE_H_190603
#define RAMENVALUE_H_190603
#include <iostream>
#include <memory>
#include <vector>
#include <cassert>
#include <optional>
#include <QString>
extern "C" {
# include <caml/mlvalues.h>
}
#include "misc.h"
#include "RamenTypeStructure.h"

/*
 * A type is a structure + the nullability flag.
 * Function input and output have types, compound types have subtypes.
 * Values have only a structure but no type.
 * It is not possible to get the type of a value as we do not know, unless
 * it's VNull, if it's nullable (and if it's VNull, then we do not know
 * it's structure).
 * It is not even possible to retrieve the structure of a value, because
 * of subfields (it is possible to retrieve the structure of scalar values
 * though).
 * But it is possible to build a possible type for any value (as
 * RamenTypes.structure_of does). This is all we really need.
 */

struct RamenValue {
  virtual ~RamenValue() {};
  virtual QString const toQString() const;
  virtual value toOCamlValue() const {
    assert(!"Unimplemented RamenValue::toOCamlValue");
  }

  // Used by conf::RamenValueValue.operator==:
  virtual bool operator==(RamenValue const &that) const {
    return typeid(this) == typeid(that);
    // Then derived types must also compare the value!
  }

  // Construct from an OCaml value of type RamenTypes.value
  static RamenValue *ofOCaml(value);

  // Used for plotting
  virtual std::optional<double> toDouble() const { return std::optional<double>(); }
  virtual RamenValue const *columnValue(size_t c) const {
    assert(0 == c);
    return this;
  }
};

struct VNull : public RamenValue {
  QString const toQString() const { return QString("NULL"); }
  value toOCamlValue() const;
};

struct VFloat : public RamenValue {
  double v;

  VFloat(double v_) : v(v_) {}
  VFloat() : VFloat(0) {}
  QString const toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return v; }
};

struct VString : public RamenValue {
  QString const v;

  VString(QString const v_) : v(v_) {}
  VString() : VString(QString()) {}
  QString const toQString() const { return v; }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VBool : public RamenValue {
  bool v;

  VBool(bool v_) : v(v_) {}
  VBool() : VBool(false) {}
  QString const toQString() const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VU8 : public RamenValue {
  uint8_t v;

  VU8(uint8_t v_) : v(v_) {}
  VU8() : VU8(0) {}
  QString const toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VU16 : public RamenValue {
  uint16_t v;

  VU16(uint16_t v_) : v(v_) {}
  VU16() : VU16(0) {}
  QString const toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VU32 : public RamenValue {
  uint32_t v;

  VU32(uint32_t v_) : v(v_) {}
  VU32() : VU32(0) {}
  QString const toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VU64 : public RamenValue {
  uint64_t v;

  VU64(uint64_t v_) : v(v_) {}
  VU64() : VU64(0) {}
  QString const toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VU128 : public RamenValue {
  uint128_t v;

  VU128(uint128_t v_) : v(v_) {}
  VU128() : VU128(0) {}
  QString const toQString() const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VI8 : public RamenValue {
  int8_t v;

  VI8(int8_t v_) : v(v_) {}
  VI8() : VI8(0) {}
  QString const toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VI16 : public RamenValue {
  int16_t v;

  VI16(int16_t v_) : v(v_) {}
  VI16() : VI16(0) {}
  QString const toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VI32 : public RamenValue {
  int32_t v;

  VI32(int32_t v_) : v(v_) {}
  VI32() : VI32(0) {}
  QString const toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VI64 : public RamenValue {
  int64_t v;

  VI64(int64_t v_) : v(v_) {}
  VI64() : VI64(0) {}
  QString const toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VI128 : public RamenValue {
  int128_t v;

  VI128(int128_t v_) : v(v_) {}
  VI128() : VI128(0) {}
  QString const toQString() const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
  virtual std::optional<double> toDouble() const { return (double)v; }
};

struct VEth : public RamenValue {
  uint64_t v;

  VEth(uint64_t v_) : v(v_) {}
  VEth() : VEth(0) {}
};

struct VIpv4 : public RamenValue {
  uint32_t v;

  VIpv4(uint32_t v_) : v(v_) {}
  VIpv4() : VIpv4(0) {}
};

struct VIpv6 : public RamenValue {
  uint128_t v;

  VIpv6(uint128_t v_) : v(v_) {}
  VIpv6() : VIpv6(0) {}
};

struct VIp : public RamenValue {
  uint128_t v;
  bool isV4;

  VIp(uint128_t v_) : v(v_), isV4(false) {}
  VIp(uint32_t v_) : v(v_), isV4(true) {}
  VIp() : VIp((uint32_t)0) {}
};

struct VCidrv4 : public RamenValue {
  VIpv4 ip;
  uint8_t mask;

  VCidrv4(uint32_t ip_, uint8_t mask_) : ip(ip_), mask(mask_) {}
  VCidrv4() : VCidrv4(0, 0) {}
};

struct VCidrv6 : public RamenValue {
  VIpv6 ip;
  uint8_t mask;

  VCidrv6(uint128_t ip_, uint8_t mask_) : ip(ip_), mask(mask_) {}
  VCidrv6() : VCidrv6(0, 0) {}
};

struct VCidr : public RamenValue {
  VIp ip;
  uint8_t mask;

  VCidr(uint128_t ip_, uint8_t mask_) : ip(ip_), mask(mask_) {}
  VCidr(uint32_t ip_, uint8_t mask_) : ip(ip_), mask(mask_) {}
  VCidr() : VCidr((uint32_t)0, 0) {}
};

struct VTuple : public RamenValue {
  std::vector<RamenValue const *> v;

  VTuple(size_t numFields) { v.reserve(numFields); }
  void append(RamenValue const *);
  virtual RamenValue const *columnValue(size_t c) const {
    if (c >= v.size()) return nullptr;
    return v[c];
  }
};

struct VVec : public RamenValue {
  std::vector<RamenValue const *> v;

  VVec(size_t dim) { v.reserve(dim); }
  void append(RamenValue const *i) {
    assert(v.size() < v.capacity());
    v.push_back(i);
  }
  virtual RamenValue const *columnValue(size_t c) const {
    if (c >= v.size()) return nullptr;
    return v[c];
  }
};

struct VList : public RamenValue {
  std::vector<RamenValue const *> v;

  void append(RamenValue const *i) { v.push_back(i); }
};

struct VRecord : public RamenValue {
  std::vector<std::pair<QString, RamenValue const *>> v;

  /* VRecord fields are unserialized in another order so we built it
   * with a setter instead of an appender: */
  VRecord(size_t numFields);
  void set(size_t idx, QString const field, RamenValue const *);
  virtual RamenValue const *columnValue(size_t c) const {
    if (c >= v.size()) return nullptr;
    return v[c].second;
  }
};

#endif
