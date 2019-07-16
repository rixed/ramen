#include <iostream>
#include <cstdio>
#include <cinttypes>
#include <QCoreApplication>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
# include <caml/threads.h>
# include <caml/callback.h>
}
#include "misc.h"
#include "RamenValue.h"

QString RamenValue::toQString() const
{
  return QString("Some ") + QStringOfRamenTypeStructure(structure);
}

value RamenValue::toOCamlValue() const
{
  assert(!"TODO: toOCamlValue");
}

bool RamenValue::operator==(RamenValue const &other) const
{
  return structure == other.structure;
}

value VNull::toOCamlValue() const
{
  CAMLparam0();
  CAMLreturn(Val_int(0)); // Do not use (int)NullType here!
}

value VFloat::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, FloatType);
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
  ret = caml_alloc(1, StringType);
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
  ret = caml_alloc(1, BoolType);
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
  ret = caml_alloc(1, U8Type);
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
  ret = caml_alloc(1, U16Type);
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
  ret = caml_alloc(1, I8Type);
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
  ret = caml_alloc(1, I16Type);
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


RamenValue *RamenValue::ofOCaml(value v_)
{
  CAMLparam1(v_);
  RamenValue *ret = nullptr;

  if (Is_block(v_)) {
    // v_ is a RamenTypes.value:
    switch (Tag_val(v_)) {
      case 0:
        ret = new VFloat(Double_val(Field(v_, 0)));
        break;
      case 1:
        ret = new VString(String_val(Field(v_, 0)));
        break;
      case 2:
        ret = new VBool(Bool_val(Field(v_, 0)));
        break;
      case 3:
        ret = new VU8(Int_val(Field(v_, 0)));
        break;
      case 4:
        ret = new VU16(Int_val(Field(v_, 0)));
        break;
      case 5:
        ret = new VU32(*(uint32_t *)Data_custom_val(Field(v_, 0)));
        break;
      case 6:
        ret = new VU64(*(uint64_t *)Data_custom_val(Field(v_, 0)));
        break;
      case 7:
        ret = new VU128(*(uint128_t *)Data_custom_val(Field(v_, 0)));
        break;
      case 8:
        ret = new VI8(Int_val(Field(v_, 0)));
        break;
      case 9:
        ret = new VI16(Int_val(Field(v_, 0)));
        break;
      case 10:
        ret = new VI32(*(int32_t *)Data_custom_val(Field(v_, 0)));
        break;
      case 11:
        ret = new VI64(*(int64_t *)Data_custom_val(Field(v_, 0)));
        break;
      case 12:
        ret = new VI128(*(int128_t *)Data_custom_val(Field(v_, 0)));
        break;
      case 13:
        ret = new VEth(*(uint64_t *)Data_custom_val(Field(v_, 0)));
        break;
      case 14:
      case 15:
      case 16:
      case 17:
      case 18:
      case 19:
      case 20:
      case 21:
      case 22:
      case 23:
        std::cout << "Unimplemented RamenValueOfOCaml for tag " << Tag_val(v_)
                  << std::endl;
        ret = new VNull();
        break;
      default:
        assert(!"Invalid tag, not a RamenValueType");
    }
  } else {
    assert(Long_val(v_) == 0);
    return new VNull();
  }

  CAMLreturnT(RamenValue *, ret);
}

#if 0
static int structureOfValueType(enum RamenValueType type)
{
  switch (type) {
    case NullType: return 5; // TAny. NULL can have any type. Good luck.
    case FloatType: return 1;
    case StringType: return 2;
    case BoolType: return 3;
    case U8Type: return 6;
    case U16Type: return 7;
    case U32Type: return 8;
    case U64Type: return 9;
    case U128Type: return 10;
    case I8Type: return 11;
    case I16Type: return 12;
    case I32Type: return 13;
    case I64Type: return 14;
    case I128Type: return 15;
    case EthType: return 16;
    case Ipv4Type: return 17;
    case Ipv6Type: return 18;
    case IpType: return 19;
    case Cidrv4Type: return 20;
    case Cidrv6Type: return 21;
    case CidrType: return 22;
    // For those RamenValueType is not enough.
    case TupleType:
    case VecType:
    case ListType:
    case RecordType:
      return 0;
    case LastRamenValueType:
      assert(!"Invalid type in structureOfValueType");
  };
  assert(!"Missing case in structureOfValueType");
}

RamenValue *RamenValue::ofQString(enum RamenValueType type, QString const &s)
{
  if (1 == caml_c_thread_register()) { // Must be done before we use local_roots!
    std::cout << "Registered new thread to OCaml" << std::endl;
  }

  {
//  return new VString("lol");
/*    CAMLparam0();
    CAMLlocal1(ret_);*/
    value ret_;
    caml_acquire_runtime_system();
    static value *valueOfString = nullptr;
    if (! valueOfString) {
      valueOfString = caml_named_value("value_of_string");
    }
    /* That function expect the structure (for instance, TBool) but type is
     * the tag of the value; So, convert: */
    int structure = structureOfValueType(type);
    ret_ = caml_callback2(*valueOfString, Val_int(structure), caml_copy_string(s.toStdString().c_str()));

    RamenValue *ret = ofOCaml(ret_);
    caml_release_runtime_system();

    //CAMLreturnT(RamenValue *, ret);
    return ret;
  }
}
#endif

RamenValue *RamenValue::ofQString(enum RamenTypeStructure structure, QString const &s)
{
  bool ok = true;
  RamenValue *ret = nullptr;
  switch (structure) {
    case EmptyType:
      assert("Cannot build a value of EmptyType");
    case FloatType:
      ret = new VFloat(s.toDouble(&ok));
      break;
    case StringType:
      ret = new VString(s);
      break;
    case BoolType:
      ret = new VBool(looks_like_true(s));
      break;
    case NumType:
    case AnyType:
      ret = new VNull();
      break;
    case U8Type:
      ret = new VU8(s.toLong(&ok));
      break;
    case U16Type:
      ret = new VU16(s.toLong(&ok));
      break;
    case U32Type:
      ret = new VU32(s.toLong(&ok));
      break;
    case U64Type:
      ret = new VU64(s.toLong(&ok));
      break;
    case U128Type:
      ret = new VU128(s.toLong(&ok));
      break;
    case I8Type:
      ret = new VI8(s.toLong(&ok));
      break;
    case I16Type:
      ret = new VI16(s.toLong(&ok));
      break;
    case I32Type:
      ret = new VI32(s.toLong(&ok));
      break;
    case I64Type:
      ret = new VI64(s.toLong(&ok));
      break;
    case I128Type:
      ret = new VI128(s.toLong(&ok));
      break;
    case EthType:
      ret = new VEth(s.toLong(&ok));
      break;
    case Ipv4Type:
    case Ipv6Type:
    case IpType:
    case Cidrv4Type:
    case Cidrv6Type:
    case CidrType:
    case TupleType:
    case VecType:
    case ListType:
    case RecordType:
      std::cout << "Unimplemented RamenValueOfOCaml for structure "
                << QStringOfRamenTypeStructure(structure).toStdString()
                << std::endl;
      ret = new VNull();
      break;
  }
  if (! ret)
    assert(!"Invalid RamenValueType");
  if (! ok)
    std::cerr << "Cannot convert " << s.toStdString() << " into a RamenValue" << std::endl;
  return ret;
}
