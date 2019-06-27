#ifndef CONFRAMENVALUE_H_190603
#define CONFRAMENVALUE_H_190603
#include <iostream>
#include <vector>
#include <QString>
extern "C" {
# include <caml/mlvalues.h>
}
#include "misc.h"

namespace conf {

enum RamenValueType {
  // VNull not a block though:
  VNullType = -1, VFloatType = 0, VStringType, VBoolType,
  VU8Type, VU16Type, VU32Type, VU64Type, VU128Type,
  VI8Type, VI16Type, VI32Type, VI64Type, VI128Type,
  VEthType, VIpv4Type, VIpv6Type, VIpType, VCidrv4Type, VCidrv6Type, VCidrType,
  VTupleType, VVecType, VListType, VRecordType,
  LastRamenValueType
};

struct RamenValue {
  RamenValueType type;
  RamenValue(RamenValueType type_) : type(type_) {}
  virtual ~RamenValue() {}
  virtual QString toQString() const;
  virtual value toOCamlValue() const;
  virtual bool operator==(RamenValue const &) const;

  // Construct from an OCaml value of type RamenTypes.value
  static RamenValue *ofOCaml(value);

  // Construct the value from a string (uses ofOCaml under the hood):
  static RamenValue *ofQString(enum RamenValueType, QString const &);
};

struct VNull : public RamenValue {
  VNull() : RamenValue(VNullType) {}
  QString toQString() const { return QString("NULL"); }
  value toOCamlValue() const;
};

struct VFloat : public RamenValue {
  double v;
  VFloat(double v_) : RamenValue(VFloatType), v(v_) {}
  VFloat() : VFloat(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VString : public RamenValue {
  QString v;
  VString() : RamenValue(VStringType) {}
  VString(QString v_) : VString() { v = v_; }
  QString toQString() const { return v; }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VBool : public RamenValue {
  bool v;
  VBool(bool v_) : RamenValue(VBoolType), v(v_) {}
  VBool() : VBool(false) {}
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU8 : public RamenValue {
  uint8_t v;
  VU8(uint8_t v_) : RamenValue(VU8Type), v(v_) {}
  VU8() : VU8(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU16 : public RamenValue {
  uint16_t v;
  VU16(uint16_t v_) : RamenValue(VU16Type), v(v_) {}
  VU16() : VU16(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU32 : public RamenValue {
  uint32_t v;
  VU32(uint32_t v_) : RamenValue(VU32Type), v(v_) {}
  VU32() : VU32(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU64 : public RamenValue {
  uint64_t v;
  VU64(uint64_t v_) : RamenValue(VU64Type), v(v_) {}
  VU64() : VU64(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VU128 : public RamenValue {
  uint128_t v;
  VU128(uint128_t v_) : RamenValue(VU128Type), v(v_) {}
  VU128() : VU128(0) {}
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI8 : public RamenValue {
  int8_t v;
  VI8(int8_t v_) : RamenValue(VI8Type), v(v_) {}
  VI8() : VI8(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI16 : public RamenValue {
  int16_t v;
  VI16(int16_t v_) : RamenValue(VI16Type), v(v_) {}
  VI16() : VI16(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI32 : public RamenValue {
  int32_t v;
  VI32(int32_t v_) : RamenValue(VI32Type), v(v_) {}
  VI32() : VI32(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI64 : public RamenValue {
  int64_t v;
  VI64(int64_t v_) : RamenValue(VI64Type), v(v_) {}
  VI64() : VI64(0) {}
  QString toQString() const { return QString::number(v); }
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VI128 : public RamenValue {
  int128_t v;
  VI128(int128_t v_) : RamenValue(VI128Type), v(v_) {}
  VI128() : VI128(0) {}
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(RamenValue const &) const;
};

struct VEth : public RamenValue {
  uint64_t v;
  VEth(uint64_t v_) : RamenValue(VEthType), v(v_) {}
  VEth() : VEth(0) {}
};

struct VIpv4 : public RamenValue {
  uint32_t v;
  VIpv4(uint32_t v_) : RamenValue(VIpv4Type), v(v_) {}
  VIpv4() : VIpv4(0) {}
};

struct VIpv6 : public RamenValue {
  uint128_t v;
  VIpv6(uint128_t v_) : RamenValue(VIpv6Type), v(v_) {}
  VIpv6() : VIpv6(0) {}
};

struct VIp : public RamenValue {
  uint128_t v;
  bool isV4;
  VIp(uint128_t v_) : RamenValue(VIpType), v(v_), isV4(false) {}
  VIp(uint32_t v_) : RamenValue(VIpType), v(v_), isV4(true) {}
  VIp() : VIp((uint32_t)0) {}
};

struct VCidrv4 : public RamenValue {
  VIpv4 ip;
  uint8_t mask;
  VCidrv4(uint32_t ip_, uint8_t mask_) :
    RamenValue(VCidrv4Type), ip(ip_), mask(mask_) {}
  VCidrv4() : VCidrv4(0, 0) {}
};

struct VCidrv6 : public RamenValue {
  VIpv6 ip;
  uint8_t mask;
  VCidrv6(uint128_t ip_, uint8_t mask_) :
    RamenValue(VCidrv6Type), ip(ip_), mask(mask_) {}
  VCidrv6() : VCidrv6(0, 0) {}
};

struct VCidr : public RamenValue {
  VIp ip;
  uint8_t mask;
  VCidr(uint128_t ip_, uint8_t mask_) :
    RamenValue(VCidrType), ip(ip_), mask(mask_) {}
  VCidr(uint32_t ip_, uint8_t mask_) :
    RamenValue(VCidrType), ip(ip_), mask(mask_) {}
  VCidr() : VCidr((uint32_t)0, 0) {}
};

struct VTuple : public RamenValue {
  std::vector<RamenValue const *> v;
  VTuple() : RamenValue(VTupleType) {};
  void append(RamenValue const *i) { v.push_back(i); }
};

struct VList : public RamenValue {
  std::vector<RamenValue const *> v;
  VList() : RamenValue(VListType) {};
  void append(RamenValue const *i) { v.push_back(i); }
};

struct VRecord : public RamenValue {
  std::vector<std::pair<QString, RamenValue const *>> v;
  VRecord() : RamenValue(VRecordType) {};
  void append(QString field, RamenValue const *i) { v.emplace_back(field, i); }
};

};

#endif
