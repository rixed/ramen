#ifndef RAMENVALUE_H_190603
#define RAMENVALUE_H_190603
#include <iostream>
#include <vector>
#include <QString>
extern "C" {
# include <caml/mlvalues.h>
}
#include "misc.h"
#include "RamenTypeStructure.h"

struct RamenValue {
  /* FIXME: once the RamenTypeStructure is a class, make a method to return
   *        the structure of a value and remove this: */
  RamenTypeStructure structure;
  RamenValue(RamenTypeStructure structure_) : structure(structure_) {}
  virtual ~RamenValue() {}
  virtual QString toQString() const;
  virtual value toOCamlValue() const;
  virtual bool operator==(RamenValue const &) const; // Used by conf::RamenValueValue.operator==

  // Construct from an OCaml value of type RamenTypes.value
  static RamenValue *ofOCaml(value);

  // Construct the value from a string (uses ofOCaml under the hood):
  static RamenValue *ofQString(enum RamenTypeStructure, QString const &);
};

struct VNull : public RamenValue {
  VNull() : RamenValue(AnyType) {}
  QString toQString() const { return QString("NULL"); }
  value toOCamlValue() const;
};

struct VFloat : public RamenValue {
  double v;
  VFloat(double v_) : RamenValue(FloatType), v(v_) {}
  VFloat() : VFloat(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VString : public RamenValue {
  QString v;
  VString() : RamenValue(StringType) {}
  VString(QString v_) : VString() { v = v_; }
  QString toQString() const { return v; }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VBool : public RamenValue {
  bool v;
  VBool(bool v_) : RamenValue(BoolType), v(v_) {}
  VBool() : VBool(false) {}
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU8 : public RamenValue {
  uint8_t v;
  VU8(uint8_t v_) : RamenValue(U8Type), v(v_) {}
  VU8() : VU8(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU16 : public RamenValue {
  uint16_t v;
  VU16(uint16_t v_) : RamenValue(U16Type), v(v_) {}
  VU16() : VU16(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU32 : public RamenValue {
  uint32_t v;
  VU32(uint32_t v_) : RamenValue(U32Type), v(v_) {}
  VU32() : VU32(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU64 : public RamenValue {
  uint64_t v;
  VU64(uint64_t v_) : RamenValue(U64Type), v(v_) {}
  VU64() : VU64(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU128 : public RamenValue {
  uint128_t v;
  VU128(uint128_t v_) : RamenValue(U128Type), v(v_) {}
  VU128() : VU128(0) {}
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI8 : public RamenValue {
  int8_t v;
  VI8(int8_t v_) : RamenValue(I8Type), v(v_) {}
  VI8() : VI8(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI16 : public RamenValue {
  int16_t v;
  VI16(int16_t v_) : RamenValue(I16Type), v(v_) {}
  VI16() : VI16(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI32 : public RamenValue {
  int32_t v;
  VI32(int32_t v_) : RamenValue(I32Type), v(v_) {}
  VI32() : VI32(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI64 : public RamenValue {
  int64_t v;
  VI64(int64_t v_) : RamenValue(I64Type), v(v_) {}
  VI64() : VI64(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI128 : public RamenValue {
  int128_t v;
  VI128(int128_t v_) : RamenValue(I128Type), v(v_) {}
  VI128() : VI128(0) {}
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VEth : public RamenValue {
  uint64_t v;
  VEth(uint64_t v_) : RamenValue(EthType), v(v_) {}
  VEth() : VEth(0) {}
};

struct VIpv4 : public RamenValue {
  uint32_t v;
  VIpv4(uint32_t v_) : RamenValue(Ipv4Type), v(v_) {}
  VIpv4() : VIpv4(0) {}
};

struct VIpv6 : public RamenValue {
  uint128_t v;
  VIpv6(uint128_t v_) : RamenValue(Ipv6Type), v(v_) {}
  VIpv6() : VIpv6(0) {}
};

struct VIp : public RamenValue {
  uint128_t v;
  bool isV4;
  VIp(uint128_t v_) : RamenValue(IpType), v(v_), isV4(false) {}
  VIp(uint32_t v_) : RamenValue(IpType), v(v_), isV4(true) {}
  VIp() : VIp((uint32_t)0) {}
};

struct VCidrv4 : public RamenValue {
  VIpv4 ip;
  uint8_t mask;
  VCidrv4(uint32_t ip_, uint8_t mask_) :
    RamenValue(Cidrv4Type), ip(ip_), mask(mask_) {}
  VCidrv4() : VCidrv4(0, 0) {}
};

struct VCidrv6 : public RamenValue {
  VIpv6 ip;
  uint8_t mask;
  VCidrv6(uint128_t ip_, uint8_t mask_) :
    RamenValue(Cidrv6Type), ip(ip_), mask(mask_) {}
  VCidrv6() : VCidrv6(0, 0) {}
};

struct VCidr : public RamenValue {
  VIp ip;
  uint8_t mask;
  VCidr(uint128_t ip_, uint8_t mask_) :
    RamenValue(CidrType), ip(ip_), mask(mask_) {}
  VCidr(uint32_t ip_, uint8_t mask_) :
    RamenValue(CidrType), ip(ip_), mask(mask_) {}
  VCidr() : VCidr((uint32_t)0, 0) {}
};

struct VTuple : public RamenValue {
  std::vector<RamenValue const *> v;
  VTuple() : RamenValue(TupleType) {};
  void append(RamenValue const *i) { v.push_back(i); }
};

struct VList : public RamenValue {
  std::vector<RamenValue const *> v;
  VList() : RamenValue(ListType) {};
  void append(RamenValue const *i) { v.push_back(i); }
};

struct VRecord : public RamenValue {
  std::vector<std::pair<QString, RamenValue const *>> v;
  VRecord() : RamenValue(RecordType) {};
  void append(QString field, RamenValue const *i) { v.emplace_back(field, i); }
};

#endif
