#include <cassert>
#include <cstdio>
#include <cstring>
#include <cinttypes>
#include <QCoreApplication>
#include <QDebug>
#include <QTextStream>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
# include <caml/threads.h>
# include <caml/callback.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
}
extern "C" {
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}
#include "misc.h"
#include "KLabel.h"
#include "KFloatEditor.h"
#include "KIntEditor.h"
#include "KLineEdit.h"
#include "KTextEdit.h"
#include "KCharEditor.h"
#include "KBool.h"
#include "RamenType.h"
#include "RamenValue.h"

enum OCamlValueTags {
  TAG_VNull = 0,
  TAG_VFloat = 0,
  TAG_VString,
  TAG_VBool,
  TAG_VChar,
  TAG_VU8,
  TAG_VU16,
  TAG_VU24,
  TAG_VU32,
  TAG_VU40,
  TAG_VU48,
  TAG_VU56,
  TAG_VU64,
  TAG_VU128,
  TAG_VI8,
  TAG_VI16,
  TAG_VI24,
  TAG_VI32,
  TAG_VI40,
  TAG_VI48,
  TAG_VI56,
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
  TAG_VLst,
  TAG_VRecord,
  TAG_VMap
};

AtomicWidget *RamenValue::editorWidget(std::string const &key, QWidget *parent) const
{
  KLabel *editor = new KLabel(parent);
  editor->setKey(key);
  return editor;
}

QDebug operator<<(QDebug debug, RamenValue const &v)
{
  QDebugStateSaver saver(debug);
  debug.nospace() << v.toQString(std::string());

  return debug;
}

// Does not alloc on the OCaml heap
value VNull::toOCamlValue() const
{
  CAMLparam0();
  checkInOCamlThread();
  CAMLreturn(Val_int(TAG_VNull)); // Do not use (int)NullType here!
}

// This _does_ alloc on the OCaml heap
value VFloat::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VFloat);
  Store_field(ret, 0, caml_copy_double(v));
  CAMLreturn(ret);
}

QString const VFloat::toQString(std::string const &key) const
{
  if (
    key == "time" ||
    endsWith(key, "/last_exit") ||
    endsWith(key, "/quarantine_until") ||
    endsWith(key, "/last_killed") ||
    endsWith(key, "/last_exit") ||
    endsWith(key, "/quarantine_until") ||
    endsWith(key, "/first_attempt") ||
    endsWith(key, "/last_attempt") ||
    endsWith(key, "/next_scheduled") ||
    endsWith(key, "/next_send")
  )
    return stringOfDate(v);
  else
    return QString::number(v, 'g', 13);
}

AtomicWidget *VFloat::editorWidget(std::string const &key, QWidget *parent) const
{
  KFloatEditor *editor =
    key == "storage/recall_cost" ?
      new KFloatEditor(parent, 0., 1.) :
      new KFloatEditor(parent);
  editor->setKey(key);
  return editor;
}

bool VFloat::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VFloat const &o = static_cast<VFloat const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VString::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VString);
  Store_field(ret, 0, caml_copy_string(v.toStdString().c_str()));
  CAMLreturn(ret);
}

AtomicWidget *VString::editorWidget(std::string const &key, QWidget *parent) const
{
  AtomicWidget *editor =
    startsWith(key, "sources/") ?
      static_cast<AtomicWidget *>(new KTextEdit(parent)) :
      static_cast<AtomicWidget *>(new KLineEdit(parent));
  editor->setKey(key);
  return editor;
}

bool VString::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VString const &o = static_cast<VString const &>(other);
  return v == o.v;
}

QString const VBool::toQString(std::string const &) const
{
  if (v)
    return QCoreApplication::translate("QMainWindow", "true");
  else
    return QCoreApplication::translate("QMainWindow", "false");
}

// This _does_ alloc on the OCaml heap
value VBool::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VBool);
  Store_field(ret, 0, Val_bool(v));
  CAMLreturn(ret);
}

AtomicWidget *VBool::editorWidget(std::string const &key, QWidget *parent) const
{
  KBool *editor = new KBool(parent);
  editor->setKey(key);
  return editor;
}

bool VBool::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VBool const &o = static_cast<VBool const &>(other);
  return v == o.v;
}

bool VChar::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VChar const &o = static_cast<VChar const &>(other);
  return v == o.v;
}

QString const VChar::toQString(std::string const &) const {
  QString res;
  QTextStream(&res) << "#\\" << v;
  return res;
}

VChar *VChar::ofQString(QString const&)
{
  // QStringRef c = s.midRef(2); // ignore first two characters "\#".
  // return new VChar(c.toInt());
  assert(!"TODO: VChar::ofQString");
}

value VChar::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VChar);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

AtomicWidget *VChar::editorWidget(std::string const &key, QWidget *parent) const
{
  KCharEditor *editor =
    new KCharEditor(&VChar::ofQString, parent);
  editor->setKey(key);
  return editor;
}

extern "C" {
  extern struct custom_operations uint128_ops;
  extern struct custom_operations uint64_ops;
  extern struct custom_operations uint32_ops;
  extern struct custom_operations int128_ops;
  extern struct custom_operations caml_int64_ops;
  extern struct custom_operations caml_int32_ops;
}

// This _does_ alloc on the OCaml heap
value VU8::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VU8);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

AtomicWidget *VU8::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VU8::ofQString, parent, 0, std::numeric_limits<uint8_t>::max());
  editor->setKey(key);
  return editor;
}

bool VU8::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU8 const &o = static_cast<VU8 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VU16::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VU16);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

AtomicWidget *VU16::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VU16::ofQString, parent, 0, std::numeric_limits<uint16_t>::max());
  editor->setKey(key);
  return editor;
}

bool VU16::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU16 const &o = static_cast<VU16 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
// U24s are just integers:
value VU24::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VU24);
  Store_field(ret, 0, Val_long(v));
  CAMLreturn(ret);
}

AtomicWidget *VU24::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VU24::ofQString, parent, 0, 16777216ULL);
  editor->setKey(key);
  return editor;
}

bool VU24::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU24 const &o = static_cast<VU24 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
// U32 are custom blocks:
value VU32::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, tmp);
  checkInOCamlThread();
  tmp = caml_alloc_custom(&uint32_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(tmp), &v, sizeof(v));
  ret = caml_alloc(1, TAG_VU32);
  Store_field(ret, 0, tmp);
  CAMLreturn(ret);
}

AtomicWidget *VU32::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VU32::ofQString, parent, 0, std::numeric_limits<uint32_t>::max());
  editor->setKey(key);
  return editor;
}

bool VU32::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU32 const &o = static_cast<VU32 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VU40::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, tmp);
  checkInOCamlThread();
  tmp = caml_alloc_custom(&uint64_ops, sizeof(v), 0, 1);
  uint64_t const vv = v << 24;
  memcpy(Data_custom_val(tmp), &vv, sizeof(vv));
  ret = caml_alloc(1, TAG_VU40);
  Store_field(ret, 0, tmp);
  CAMLreturn(ret);
}

AtomicWidget *VU40::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VU40::ofQString, parent, 0, 1099511627776ULL);
  editor->setKey(key);
  return editor;
}

bool VU40::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU40 const &o = static_cast<VU40 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VU48::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, tmp);
  checkInOCamlThread();
  tmp = caml_alloc_custom(&uint64_ops, sizeof(v), 0, 1);
  uint64_t const vv = v << 16;
  memcpy(Data_custom_val(tmp), &vv, sizeof(vv));
  ret = caml_alloc(1, TAG_VU48);
  Store_field(ret, 0, tmp);
  CAMLreturn(ret);
}

AtomicWidget *VU48::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VU48::ofQString, parent, 0, 281474976710656ULL);
  editor->setKey(key);
  return editor;
}

bool VU48::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU48 const &o = static_cast<VU48 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VU56::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, tmp);
  checkInOCamlThread();
  tmp = caml_alloc_custom(&uint64_ops, sizeof(v), 0, 1);
  uint64_t const vv = v << 8;
  memcpy(Data_custom_val(tmp), &vv, sizeof(vv));
  ret = caml_alloc(1, TAG_VU64);
  Store_field(ret, 0, tmp);
  CAMLreturn(ret);
}

AtomicWidget *VU56::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VU56::ofQString, parent, 0, 72057594037927936ULL);
  editor->setKey(key);
  return editor;
}

bool VU56::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU56 const &o = static_cast<VU56 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VU64::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, tmp);
  checkInOCamlThread();
  tmp = caml_alloc_custom(&uint64_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(tmp), &v, sizeof(v));
  ret = caml_alloc(1, TAG_VU64);
  Store_field(ret, 0, tmp);
  CAMLreturn(ret);
}

AtomicWidget *VU64::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VU64::ofQString, parent, 0, std::numeric_limits<uint64_t>::max());
  editor->setKey(key);
  return editor;
}

bool VU64::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU64 const &o = static_cast<VU64 const &>(other);
  return v == o.v;
}

QString const VU128::toQString(std::string const &) const
{
  char s[] = "000000000000000000000000000000000000000";
  std::snprintf(s, sizeof(s), "%016" PRIx64 "%016" PRIx64, (uint64_t)(v >> 64), (uint64_t)v);
  return QString(s);
}

// This _does_ alloc on the OCaml heap
value VU128::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, tmp);
  checkInOCamlThread();
  tmp = caml_alloc_custom(&uint128_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(tmp), &v, sizeof(v));
  ret = caml_alloc(1, TAG_VU128);
  Store_field(ret, 0, tmp);
  CAMLreturn(ret);
}

AtomicWidget *VU128::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VU128::ofQString, parent, 0, std::numeric_limits<uint128_t>::max());
  editor->setKey(key);
  return editor;
}

bool VU128::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VU128 const &o = static_cast<VU128 const &>(other);
  return v == o.v;
}

// Does not alloc on the OCaml heap
value VI8::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VI8);
  Store_field(ret, 0, Val_int(v));
  CAMLreturn(ret);
}

AtomicWidget *VI8::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VI8::ofQString, parent, std::numeric_limits<int8_t>::min(), std::numeric_limits<uint8_t>::max());
  editor->setKey(key);
  return editor;
}

bool VI8::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI8 const &o = static_cast<VI8 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VI16::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VI16);
# ifdef ARCH_SIXTYFOUR
  Store_field(ret, 0, (((intnat)(v) << 48) + 1));
# else
  Store_field(ret, 0, (((intnat)(v) << 16) + 1));
# endif
  CAMLreturn(ret);
}

AtomicWidget *VI16::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VI16::ofQString, parent, std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());
  editor->setKey(key);
  return editor;
}

bool VI16::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI16 const &o = static_cast<VI16 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VI24::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VI24);
# ifdef ARCH_SIXTYFOUR
  Store_field(ret, 0, (((intnat)(v) << 40) + 1));
# else
  Store_field(ret, 0, (((intnat)(v) << 8) + 1));
# endif
  CAMLreturn(ret);
}

AtomicWidget *VI24::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VI24::ofQString, parent, -8388608LL, 8388607);
  editor->setKey(key);
  return editor;
}

bool VI24::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI24 const &o = static_cast<VI24 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
// I32 are custom blocks:
value VI32::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VI32);
  Store_field(ret, 0, caml_copy_int32(v));
  CAMLreturn(ret);
}

AtomicWidget *VI32::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VI32::ofQString, parent, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
  editor->setKey(key);
  return editor;
}

bool VI32::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI32 const &o = static_cast<VI32 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VI40::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VI40);
  int64_t const vv = v << 24;
  Store_field(ret, 0, caml_copy_int64(vv));
  CAMLreturn(ret);
}

AtomicWidget *VI40::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VI40::ofQString, parent, -549755813888LL, 549755813887LL);
  editor->setKey(key);
  return editor;
}

bool VI40::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI40 const &o = static_cast<VI40 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VI48::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VI48);
  int64_t const vv = v << 16;
  Store_field(ret, 0, caml_copy_int64(vv));
  CAMLreturn(ret);
}

AtomicWidget *VI48::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VI48::ofQString, parent, -140737488355328LL, 140737488355327LL);
  editor->setKey(key);
  return editor;
}

bool VI48::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI48 const &o = static_cast<VI48 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VI56::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VI56);
  int64_t const vv = v << 8;
  Store_field(ret, 0, caml_copy_int64(vv));
  CAMLreturn(ret);
}

AtomicWidget *VI56::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VI56::ofQString, parent, -36028797018963968LL, 36028797018963967LL);
  editor->setKey(key);
  return editor;
}

bool VI56::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI56 const &o = static_cast<VI56 const &>(other);
  return v == o.v;
}

// This _does_ alloc on the OCaml heap
value VI64::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, TAG_VI64);
  Store_field(ret, 0, caml_copy_int64(v));
  CAMLreturn(ret);
}

AtomicWidget *VI64::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VI64::ofQString, parent, std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
  editor->setKey(key);
  return editor;
}

bool VI64::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI64 const &o = static_cast<VI64 const &>(other);
  return v == o.v;
}

QString const VI128::toQString(std::string const &) const
{
  char s[] = "-000000000000000000000000000000000000000";
  uint128_t v_ = v >= 0 ? v : -v;
  std::snprintf(s + 1, sizeof(s) - 1, "%016" PRIx64 "%016" PRIx64, (uint64_t)(v_ >> 64), (uint64_t)v_);
  return QString(v >= 0 ? s+1 : s);
}

// This _does_ alloc on the OCaml heap
value VI128::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, tmp);
  checkInOCamlThread();
  tmp = caml_alloc_custom(&int128_ops, sizeof(v), 0, 1);
  memcpy(Data_custom_val(tmp), &v, sizeof(v));
  ret = caml_alloc(1, TAG_VI128);
  Store_field(ret, 0, tmp);
  CAMLreturn(ret);
}

AtomicWidget *VI128::editorWidget(std::string const &key, QWidget *parent) const
{
  KIntEditor *editor =
    new KIntEditor(&VI128::ofQString, parent, std::numeric_limits<int128_t>::min(), std::numeric_limits<int128_t>::max());
  editor->setKey(key);
  return editor;
}

bool VI128::operator==(RamenValue const &other) const
{
  if (! RamenValue::operator==(other)) return false;
  VI128 const &o = static_cast<VI128 const &>(other);
  return v == o.v;
}

QString const VEth::toQString(std::string const &) const
{
  return QString("Some VEth");
}

static QString qstringOfIpv4(uint32_t const v)
{
  return QString(inet_ntoa((struct in_addr){ .s_addr = htonl(v) }));
}

static QString qstringOfIpv6(uint128_t const v)
{
  char buf[ 8*(4+1) ];
  struct in6_addr ip;

# if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  memcpy(ip.s6_addr, &v, sizeof(ip.s6_addr));
# else
  uint8_t const *p { reinterpret_cast<uint8_t const *>(&v) };
  for (size_t i = 0; i < 16; ++i) ip.s6_addr[i] = p[15-i];
# endif

  return QString(inet_ntop(AF_INET6, &ip, buf, sizeof buf));
}

QString const VIpv4::toQString(std::string const &) const
{
  return qstringOfIpv4(v);
}

QString const VIpv6::toQString(std::string const &) const
{
  return qstringOfIpv6(v);
}

QString const VIp::toQString(std::string const &) const
{
  return isV4 ? qstringOfIpv4(v) : qstringOfIpv6(v);
}

QString const VCidrv4::toQString(std::string const &k) const
{
  return ip.toQString(k) + QString('/') + QString::number(mask);
}

QString const VCidrv6::toQString(std::string const &k) const
{
  return ip.toQString(k) + QString('/') + QString::number(mask);
}

QString const VCidr::toQString(std::string const &k) const
{
  return ip.toQString(k) + QString('/') + QString::number(mask);
}

RamenValue *RamenValue::ofOCaml(value v_)
{
  CAMLparam1(v_);
  RamenValue *ret = nullptr;

  if (Is_block(v_)) {
    // v_ is a RamenTypes.value:
    switch ((OCamlValueTags)Tag_val(v_)) {
      case TAG_VFloat:
        ret = new VFloat(Double_val(Field(v_, 0)));
        break;
      case TAG_VString:
        ret = new VString(String_val(Field(v_, 0)));
        break;
      case TAG_VBool:
        ret = new VBool(Bool_val(Field(v_, 0)));
        break;
      case TAG_VChar:
        ret = new VChar(Int_val(Field(v_, 0)));
        break;
      case TAG_VU8:
        ret = new VU8(Int_val(Field(v_, 0)));
        break;
      case TAG_VU16:
        ret = new VU16(Unsigned_long_val(Field(v_, 0)));
        break;
      case TAG_VU24:
        ret = new VU24(Unsigned_long_val(Field(v_, 0)));
        break;
      case TAG_VU32:
        ret = new VU32(*(uint32_t *)Data_custom_val(Field(v_, 0)));
        break;
      case TAG_VU40:
        ret = new VU40((*(uint64_t *)Data_custom_val(Field(v_, 0))) >> 24);
        break;
      case TAG_VU48:
        ret = new VU48((*(uint64_t *)Data_custom_val(Field(v_, 0))) >> 16);
        break;
      case TAG_VU56:
        ret = new VU56((*(uint64_t *)Data_custom_val(Field(v_, 0))) >> 8);
        break;
      case TAG_VU64:
        ret = new VU64(*(uint64_t *)Data_custom_val(Field(v_, 0)));
        break;
      case TAG_VU128:
        ret = new VU128(*(uint128_t *)Data_custom_val(Field(v_, 0)));
        break;
      case TAG_VI8:
        ret = new VI8(Int_val(Field(v_, 0)));
        break;
      case TAG_VI16:
        ret = new VI16(
#       ifdef ARCH_SIXTYFOUR
          (int16_t)(((intnat)(Field(v_, 0))) >> 48)
#       else
          (int16_t)(((intnat)(Field(v_, 0))) >> 16)
#       endif
        );
        break;
      case TAG_VI24:
        ret = new VI24(
#       ifdef ARCH_SIXTYFOUR
          (int16_t)(((intnat)(Field(v_, 0))) >> 40)
#       else
          (int16_t)(((intnat)(Field(v_, 0))) >> 8)
#       endif
        );
        break;
      case TAG_VI32:
        ret = new VI32(*(int32_t *)Data_custom_val(Field(v_, 0)));
        break;
      case TAG_VI40:
        ret = new VI40((*(int64_t *)Data_custom_val(Field(v_, 0))) >> 24);
        break;
      case TAG_VI48:
        ret = new VI48((*(int64_t *)Data_custom_val(Field(v_, 0))) >> 16);
        break;
      case TAG_VI56:
        ret = new VI56((*(int64_t *)Data_custom_val(Field(v_, 0))) >> 8);
        break;
      case TAG_VI64:
        ret = new VI64(*(int64_t *)Data_custom_val(Field(v_, 0)));
        break;
      case TAG_VI128:
        ret = new VI128(*(int128_t *)Data_custom_val(Field(v_, 0)));
        break;
      case TAG_VEth:
        ret = new VEth(*(uint64_t *)Data_custom_val(Field(v_, 0)));
        break;
      case TAG_VIpv4:
        ret = new VIpv4(*(uint32_t *)Data_custom_val(Field(v_, 0)));
        break;
      case TAG_VIpv6:
        ret = new VIpv6(*(uint128_t *)Data_custom_val(Field(v_, 0)));
        break;
      case TAG_VIp:
        assert(Is_block(Field(v_, 0)));
        if (Tag_val(Field(v_, 0)) == 0) { // Ipv4
          ret = new VIp(*(uint32_t *)Data_custom_val(Field(Field(v_, 0), 0)));
        } else {
          ret = new VIp(*(uint128_t *)Data_custom_val(Field(Field(v_, 0), 0)));
        }
        break;
      case TAG_VCidrv4:
      case TAG_VCidrv6:
      case TAG_VCidr:
        qDebug() << "Unimplemented RamenValueOfOCaml for tag"
                 << (unsigned)Tag_val(v_);
        ret = new VNull();
        break;
      case TAG_VTuple:
        ret = new VTuple(Field(v_, 0));
        break;
      case TAG_VVec:
        ret = new VVec(Field(v_, 0));
        break;
      case TAG_VLst:
        ret = new VLst(Field(v_, 0));
        break;
      case TAG_VRecord:
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

QString const VTuple::toQString(std::string const &k) const
{
  QString s;
  for (auto const &val : v) {
    if (s.length() > 0) s += ", ";
    s += val->toQString(k);
  }
  return QString("(") + s + QString(")");
}

void VTuple::append(RamenValue const *i)
{
  /* FIXME: Make sure we do not add a Null immediate and pretend the field is
   * not nullable: */
  assert(v.size() < v.capacity());
  v.push_back(i);
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

QString const VVec::toQString(std::string const &k) const
{
  QString s;
  for (auto const &val : v) {
    if (s.length() > 0) s += ", ";
    s += val->toQString(k);
  }
  return QString("[") + s + QString("]");
}

/*
 * VLst
 */

VLst::VLst(value v_)
{
  CAMLparam1(v_);
  size_t numFields = Wosize_val(v_);
  v.reserve(numFields);
  for (unsigned i = 0; i < numFields; i ++)
    append(ofOCaml(Field(v_, i)));
  CAMLreturn0;
}

QString const VLst::toQString(std::string const &k) const
{
  QString s;
  for (auto const &val : v) {
    if (s.length() > 0) s += ", ";
    s += val->toQString(k);
  }
  return QString("[") + s + QString("]");
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

QString const VRecord::toQString(std::string const &k) const
{
  QString s;
  for (auto const &val : v) {
    if (s.length() > 0) s += ", ";
    s += val.first + ":" + val.second->toQString(k);
  }
  return QString("{") + s + QString("}");
}

void VRecord::set(size_t idx, QString const field, RamenValue const *i)
{
  assert(idx < v.size());
  v[idx].first = field;
  v[idx].second = i;
}

/*
 * VSum
 */

VSum::VSum(value)
{
  // TODO
  assert(false);
}

VSum::VSum(size_t label_, QString const &cstrName_, RamenValue const *v_) :
  label(label_), cstrName(cstrName_), v(v_)
{
}

QString const VSum::toQString(std::string const &k) const
{
  return cstrName + QString(" ") + v->toQString(k);
}

std::thread::id ocamlThreadId;
extern inline void checkInOCamlThread();
