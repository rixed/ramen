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
#include "confKey.h"
#include "KLabel.h"
#include "KFloatEditor.h"
#include "KIntEditor.h"
#include "KLineEdit.h"
#include "KBool.h"
#include "RamenType.h"
#include "RamenValue.h"

enum OCamlValueTags {
  TAG_VNull = 0,
  TAG_VFloat = 0,
  TAG_VString,
  TAG_VBool,
  TAG_VU8,
  TAG_VU16,
  TAG_VU32,
  TAG_VU64,
  TAG_VU128,
  TAG_VI8,
  TAG_VI16,
  TAG_VI32,
  TAG_VI64,
  TAG_VI128,
  TAG_VEth,
  TAG_VIpv4,
  TAG_VIpv6,
  TAG_VIp,
  TAG_VCidrv4,
  TAG_VCidrv6,
  TAG_VCidr,
  TAG_VTuple,
  TAG_VVec,
  TAG_VList,
  TAG_VRecord
};

QString const RamenValue::toQString() const
{
  return QString("Some value which printer is unimplemented");
}

AtomicWidget *RamenValue::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KLabel(key, false, parent);
}

value VNull::toOCamlValue() const
{
  CAMLparam0();
  CAMLreturn(Val_int(TAG_VNull)); // Do not use (int)NullType here!
}

value VFloat::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  ret = caml_alloc(1, TAG_VFloat);
  Store_field(ret, 0, caml_copy_double(v));
  CAMLreturn(ret);
}

AtomicWidget *VFloat::editorWidget(conf::Key const &key, QWidget *parent) const
{
  if (key.s == "storage/recall_cost") {
    return new KFloatEditor(key, parent, 0., 1.);
  } else {
    return new KFloatEditor(key, parent);
  }
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
  ret = caml_alloc(1, TAG_VString);
  Store_field(ret, 0, caml_copy_string(v.toStdString().c_str()));
  CAMLreturn(ret);
}

AtomicWidget *VString::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KLineEdit(key, parent);
}

bool VString::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VString const &o = static_cast<VString const &>(other);
  return v == o.v;
}

QString const VBool::toQString() const
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
  ret = caml_alloc(1, TAG_VBool);
  Store_field(ret, 0, Val_bool(v));
  CAMLreturn(ret);
}

AtomicWidget *VBool::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KBool(key, parent);
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
  ret = caml_alloc(1, TAG_VU8);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

AtomicWidget *VU8::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VU8::ofQString, key, parent, 0, std::numeric_limits<uint8_t>::max());
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
  ret = caml_alloc(1, TAG_VU16);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

AtomicWidget *VU16::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VU16::ofQString, key, parent, 0, std::numeric_limits<uint16_t>::max());
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

AtomicWidget *VU32::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VU32::ofQString, key, parent, 0, std::numeric_limits<uint32_t>::max());
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

AtomicWidget *VU64::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VU64::ofQString, key, parent, 0, std::numeric_limits<uint64_t>::max());
}

bool VU64::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU64 const &o = static_cast<VU64 const &>(other);
  return v == o.v;
}

QString const VU128::toQString() const
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

AtomicWidget *VU128::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VU128::ofQString, key, parent, 0, std::numeric_limits<uint128_t>::max());
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
  ret = caml_alloc(1, TAG_VI8);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

AtomicWidget *VI8::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VI8::ofQString, key, parent, std::numeric_limits<int8_t>::min(), std::numeric_limits<uint8_t>::max());
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
  ret = caml_alloc(1, TAG_VI16);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

AtomicWidget *VI16::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VI16::ofQString, key, parent, std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());
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

AtomicWidget *VI32::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VI32::ofQString, key, parent, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
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

AtomicWidget *VI64::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VI64::ofQString, key, parent, std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
}

bool VI64::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI64 const &o = static_cast<VI64 const &>(other);
  return v == o.v;
}

QString const VI128::toQString() const
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

AtomicWidget *VI128::editorWidget(conf::Key const &key, QWidget *parent) const
{
  return new KIntEditor(&VI128::ofQString, key, parent, std::numeric_limits<int128_t>::min(), std::numeric_limits<int128_t>::max());
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
        std::cout << "Unimplemented RamenValueOfOCaml for tag "
                  << (unsigned)Tag_val(v_) << std::endl;
        ret = new VNull();
        break;
      case 20:
        ret = new VTuple(Field(v_, 0));
        break;
      case 21:
        ret = new VVec(Field(v_, 0));
        break;
      case 22:
        ret = new VList(Field(v_, 0));
        break;
      case 23:
        ret = new VRecord(Field(v_, 0));
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

/*
 * VTuple
 */

VTuple::VTuple(value v_)
{
  CAMLparam1(v_);
  size_t numFields = Wosize_val(v_);
  v.reserve(numFields);
  for (unsigned i = 0; i < numFields; i ++)
    append(ofOCaml(Field(v_, i)));
  CAMLreturn0;
}

void VTuple::append(RamenValue const *i)
{
  /* FIXME: Make sure we do not add a Null immediate and pretend the field is
   * not nullable: */
  assert(v.size() < v.capacity());
  v.push_back(i);
}

/*
 * VRecord
 */

VRecord::VRecord(value v_)
{
  CAMLparam1(v_);
  size_t numFields = Wosize_val(v_);
  v.reserve(numFields);
  // In an OCaml value, fields are ordered in user order:
  for (unsigned i = 0; i < numFields; i ++) {
    value pair_ = Field(v_, i);
    v.emplace_back(String_val(Field(pair_, 0)), ofOCaml(Field(pair_, 1)));
  }
  CAMLreturn0;
}

VRecord::VRecord(size_t numFields)
{
  while (numFields --) v.emplace_back(QString(), nullptr);
}

void VRecord::set(size_t idx, QString const field, RamenValue const *i)
{
  assert(idx < v.size());
  v[idx].first = field;
  v[idx].second = i;
}

/*
 * VVec
 */

VVec::VVec(value v_)
{
  CAMLparam1(v_);
  size_t numFields = Wosize_val(v_);
  v.reserve(numFields);
  for (unsigned i = 0; i < numFields; i ++)
    append(ofOCaml(Field(v_, i)));
  CAMLreturn0;
}

/*
 * VList
 */

VList::VList(value v_)
{
  CAMLparam1(v_);
  size_t numFields = Wosize_val(v_);
  v.reserve(numFields);
  for (unsigned i = 0; i < numFields; i ++)
    append(ofOCaml(Field(v_, i)));
  CAMLreturn0;
}

QString const VList::toQString () const
{
  QString s("");
  for (auto val : v) {
    if (s.length() > 0) s += ", ";
    s += val->toQString();
  }
  return QString("[") + s + QString("]");
}
