#include <iostream>
#include <cstdio>
#include <cinttypes>
#include <QCoreApplication>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
}
#include "confRamenValue.h"

namespace conf {

static QString QStringOfRamenValueType(bool isBlock, enum RamenValueType t)
{
  switch (t) {
    case VNullType:
      if (isBlock) return QString("VFloat");
      else return QString("VNull");
    case VStringType: return QString("VString");
    case VBoolType: return QString("VBool");
    case VU8Type: return QString("VU8");
    case VU16Type: return QString("VU16");
    case VU32Type: return QString("VU32");
    case VU64Type: return QString("VU64");
    case VU128Type: return QString("VU128");
    case VI8Type: return QString("VI8");
    case VI16Type: return QString("VI16");
    case VI32Type: return QString("VI32");
    case VI64Type: return QString("VI64");
    case VI128Type: return QString("VI128");
    case VEthType: return QString("VEth");
    case VIpv4Type: return QString("VIpv4");
    case VIpv6Type: return QString("VIpv6");
    case VIpType: return QString("VIp");
    case VCidrv4Type: return QString("Cidrv4");
    case VCidrv6Type: return QString("Cidrv6");
    case VCidrType: return QString("Cidr");
    case VTupleType: return QString("VTuple");
    case VVecType: return QString("VVec");
    case VListType: return QString("VList");
    case VRecordType: return QString("VRecord");
    case LastRamenValueType: assert(!"invalid RamenValueType");
  }
}

QString RamenValue::toQString() const
{
  return QString("Some ") + QStringOfRamenValueType(true, type);
}

value RamenValue::toOCamlValue() const
{
  assert(!"TODO: toOCamlValue");
}

bool RamenValue::operator==(RamenValue const &other) const
{
  return type == other.type;
}

value VNull::toOCamlValue() const
{
  CAMLparam0();
  CAMLreturn(Val_int(0));
}

value VFloat::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, VFloatType);
  Store_field(ret, 0, caml_copy_double(v));
  CAMLreturn(ret);
}

bool VFloat::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VFloat const &o = static_cast<VFloat const &>(other);
  return v == o.v;
}

value VString::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, VStringType);
  Store_field(ret, 0, caml_copy_string(v.toStdString().c_str()));
  CAMLreturn(ret);
}

bool VString::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VString const &o = static_cast<VString const &>(other);
  return v == o.v;
}

QString VBool::toQString() const
{
  if (v)
    return QCoreApplication::translate("QMainWindow", "true");
  else
    return QCoreApplication::translate("QMainWindow", "false");
}

value VBool::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, VBoolType);
  Store_field(ret, 0, Val_bool(v));
  CAMLreturn(ret);
}

bool VBool::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VBool const &o = static_cast<VBool const &>(other);
  return v == o.v;
}

extern "C" {
  extern struct custom_operations uint128_ops;
  extern struct custom_operations uint64_ops;
  extern struct custom_operations uint32_ops;
  extern struct custom_operations int128_ops;
  extern struct custom_operations caml_int64_ops;
  extern struct custom_operations caml_int32_ops;
}

value VU8::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, VU8Type);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

bool VU8::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU8 const &o = static_cast<VU8 const &>(other);
  return v == o.v;
}

value VU16::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, VU16Type);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

bool VU16::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU16 const &o = static_cast<VU16 const &>(other);
  return v == o.v;
}

// U32 are custom blocks:
value VU32::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc_custom(&uint32_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(ret), &v, sizeof(v));
  CAMLreturn(ret);
}

bool VU32::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU32 const &o = static_cast<VU32 const &>(other);
  return v == o.v;
}

value VU64::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc_custom(&uint64_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(ret), &v, sizeof(v));
  CAMLreturn(ret);
}

bool VU64::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU64 const &o = static_cast<VU64 const &>(other);
  return v == o.v;
}

QString VU128::toQString() const
{
  char s[] = "000000000000000000000000000000000000000";
  std::snprintf(s, sizeof(s), "%016" PRIx64 "%016" PRIx64, (uint64_t)(v >> 64), (uint64_t)v);
  return QString(s);
}

value VU128::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc_custom(&uint128_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(ret), &v, sizeof(v));
  CAMLreturn(ret);
}

bool VU128::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU128 const &o = static_cast<VU128 const &>(other);
  return v == o.v;
}

value VI8::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, VI8Type);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

bool VI8::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI8 const &o = static_cast<VI8 const &>(other);
  return v == o.v;
}

value VI16::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, VI16Type);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

bool VI16::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI16 const &o = static_cast<VI16 const &>(other);
  return v == o.v;
}

// U32 are custom blocks:
value VI32::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc_custom(&caml_int32_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(ret), &v, sizeof(v));
  CAMLreturn(ret);
}

bool VI32::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI32 const &o = static_cast<VI32 const &>(other);
  return v == o.v;
}

value VI64::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc_custom(&caml_int64_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(ret), &v, sizeof(v));
  CAMLreturn(ret);
}

bool VI64::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI64 const &o = static_cast<VI64 const &>(other);
  return v == o.v;
}

QString VI128::toQString() const
{
  char s[] = "-000000000000000000000000000000000000000";
  uint128_t v_ = v >= 0 ? v : -v;
  std::snprintf(s + 1, sizeof(s) - 1, "%016" PRIx64 "%016" PRIx64, (uint64_t)(v_ >> 64), (uint64_t)v_);
  return QString(v >= 0 ? s+1 : s);
}

value VI128::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc_custom(&int128_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(ret), &v, sizeof(v));
  CAMLreturn(ret);
}

bool VI128::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI128 const &o = static_cast<VI128 const &>(other);
  return v == o.v;
}


extern RamenValue *RamenValueOfOCaml(value v_)
{
  CAMLparam1(v_);
  RamenValue *ret = nullptr;

  if (Is_block(v_)) {
    RamenValueType valueType((RamenValueType)Tag_val(v_));
    switch (valueType) {
      case VFloatType:
        ret = new VFloat(Double_val(Field(v_, 0)));
        break;
      case VStringType:
        ret = new VString(String_val(Field(v_, 0)));
        break;
      case VBoolType:
        ret = new VBool(Bool_val(Field(v_, 0)));
        break;
      case VU8Type:
        ret = new VU8(Int_val(Field(v_, 0)));
        break;
      case VU16Type:
        ret = new VU16(Int_val(Field(v_, 0)));
        break;
      case VU32Type:
        ret = new VU32(*(uint32_t *)Data_custom_val(Field(v_, 0)));
        break;
      case VU64Type:
        ret = new VU64(*(uint64_t *)Data_custom_val(Field(v_, 0)));
        break;
      case VU128Type:
        ret = new VU128(*(uint128_t *)Data_custom_val(Field(v_, 0)));
        break;
      case VI8Type:
        ret = new VI8(Int_val(Field(v_, 0)));
        break;
      case VI16Type:
        ret = new VI16(Int_val(Field(v_, 0)));
        break;
      case VI32Type:
        ret = new VI32(*(int32_t *)Data_custom_val(Field(v_, 0)));
        break;
      case VI64Type:
        ret = new VI64(*(int64_t *)Data_custom_val(Field(v_, 0)));
        break;
      case VI128Type:
        ret = new VI128(*(int128_t *)Data_custom_val(Field(v_, 0)));
        break;
      case VEthType:
        ret = new VEth(*(uint64_t *)Data_custom_val(Field(v_, 0)));
        break;
      case VIpv4Type:
      case VIpv6Type:
      case VIpType:
      case VCidrv4Type:
      case VCidrv6Type:
      case VCidrType:
      case VTupleType:
      case VVecType:
      case VListType:
      case VRecordType:
        std::cout << "Unimplemented RamenValueOfOCaml for type "
                  << QStringOfRamenValueType(true, valueType).toStdString()
                  << std::endl;
        ret = new VNull();
        break;
      case LastRamenValueType:
      default:
        assert(!"Invalid tag, not a RamenValueType");
    }
  } else {
    assert(Long_val(v_) == VNullType);
    return new VNull();
  }

  CAMLreturnT(RamenValue *, ret);
}

};
