#include <cassert>
#include <cstring>
#include <iostream>
#include <QtGlobal>
#include <QDebug>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
# undef alloc
}
#include "misc.h"
#include "RamenType.h"
#include "RamenValue.h"
#include "DessserValueType.h"

static bool const verbose(false);

/*
 * Helpers for serialization
 */

// Returns the number of words required to store that many bytes:
static size_t roundUpWords(size_t sz)
{
  return (sz + 3) >> 2;
}

// Returns the number of bytes required to store that many bits:
static size_t roundUpBytes(size_t bits)
{
  return (bits + 7) >> 3;
}

// TODO: an actual object with an end to check against
static bool bitSet(unsigned char const *nullmask, unsigned null_i)
{
  if (null_i >= 8) return bitSet(nullmask + 1, null_i - 8);
  else return (*nullmask) & (1 << null_i);
}

value DessserValueType::toOCamlValue() const
{
  qCritical() << "Unimplemented conversion to OCaml value from"
              << toQString();
  assert(!"Don't know how to convert from a DessserValueType");
}

/*
 * Unknown
 */

RamenValue *Unknown::unserialize(uint32_t const *&, uint32_t const *, bool) const
{
  assert(false);
}

/*
 * Machine types
 */

/*
 * TFloat
 */

RamenValue *TFloat::valueOfQString(QString const s) const
{
  bool ok = true;
  double v = s.toDouble(&ok);
  return ok ? new VFloat(v) : nullptr;
}

RamenValue *TFloat::unserialize(uint32_t const *&start, uint32_t const *max, bool) const
{
  if (start + 2 > max) {
    qCritical() << "Cannot unserialize a float";
    return nullptr;
  }

  double v;
  static_assert(sizeof(double) <= 2 * sizeof(uint32_t));
  memcpy(&v, start, sizeof(v));
  if (verbose) qDebug() << "float =" << v;
  start += 2;

  return new VFloat(v);
}

/*
 * TString
 */

RamenValue *TString::valueOfQString(QString const s) const
{
  return new VString(s);
}

RamenValue *TString::unserialize(uint32_t const *&start, uint32_t const *max, bool) const
{
  if (start + 1 > max) {
    qCritical() << "Cannot unserialize a string";
    return nullptr;
  }

  size_t const len = *(start++);
  size_t const wordLen = roundUpWords(len);
  if (start + wordLen > max) {
    qCritical() << "Cannot unserialize of length" << len;
    return nullptr;
  }

  QString v;
  char const *c = (char const *)start;
  for (size_t i = 0; i < len; i++) {
    v.append(QChar(c[i]));
  }
  if (verbose) qDebug() << "string =" << v;
  start += roundUpWords(len);

  return new VString(v);
}

/*
 * TBool
 */

RamenValue *TBool::valueOfQString(QString const s) const
{
  return new VBool(looks_like_true(s));
}

RamenValue *TBool::unserialize(uint32_t const *&start, uint32_t const *max, bool) const
{
  if (start + 1 > max) {
    qCritical() << "Cannot unserialize a bool";
    return nullptr;
  }

  bool const v(!! *start);
  if (verbose) qDebug() << "bool =" << v;
  start += 1;

  return new VBool(v);
}

/*
 * TChar
 */

RamenValue *TChar::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VChar(v) : nullptr;
}

#define UNSERIALIZE(TT, TV, TC, words, name) \
  RamenValue *TT::unserialize(uint32_t const *&start, uint32_t const *max, bool) const \
  { \
    if (start + words > max) { \
      qCritical() << "Cannot unserialize a "# name; \
      return nullptr; \
    } \
  \
    TC const v = *(TC const *)start; \
    start += words; \
  \
    return new TV(v); \
  }

UNSERIALIZE(TChar, VChar, char, 1, "char")

/*
 * TU8
 */

RamenValue *TU8::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU8(v) : nullptr;
}

UNSERIALIZE(TU8, VU8, uint8_t, 1, "u8")

/*
 * U16
 */

RamenValue *TU16::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU16(v) : nullptr;
}

UNSERIALIZE(TU16, VU16, uint16_t, 1, "u16")

/*
 * U24
 */

RamenValue *TU24::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU24(v) : nullptr;
}

UNSERIALIZE(TU24, VU24, uint32_t, 1, "u24")

/*
 * U32
 */

RamenValue *TU32::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU32(v) : nullptr;
}

UNSERIALIZE(TU32, VU32, uint32_t, 1, "u32")

/*
 * TU40
 */

RamenValue *TU40::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU40(v) : nullptr;
}

UNSERIALIZE(TU40, VU40, uint64_t, 2, "u40")

/*
 * TU48
 */

RamenValue *TU48::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU48(v) : nullptr;
}

UNSERIALIZE(TU48, VU48, uint64_t, 2, "u48")

/*
 * TU56
 */

RamenValue *TU56::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU56(v) : nullptr;
}

UNSERIALIZE(TU56, VU56, uint64_t, 2, "u56")

/*
 * TU64
 */

RamenValue *TU64::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU64(v) : nullptr;
}

UNSERIALIZE(TU64, VU64, uint64_t, 2, "u64")

/*
 * TU128
 */

RamenValue *TU128::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU128(v) : nullptr;
}

UNSERIALIZE(TU128, VU128, uint128_t, 4, "u128")

/*
 * TI8
 */

RamenValue *TI8::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VI8(v) : nullptr;
}

UNSERIALIZE(TI8, VI8, int8_t, 1, "i8")

/*
 * TI16
 */

RamenValue *TI16::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VI16(v) : nullptr;
}

UNSERIALIZE(TI16, VI16, int16_t, 1, "i16")

/*
 * TI24
 */

RamenValue *TI24::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VI24(v) : nullptr;
}

UNSERIALIZE(TI24, VI24, int32_t, 1, "i24")

/*
 * TI32
 */

RamenValue *TI32::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VI32(v) : nullptr;
}

UNSERIALIZE(TI32, VI32, int32_t, 1, "i32")

/*
 * TI40
 */

RamenValue *TI40::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VI40(v) : nullptr;
}

UNSERIALIZE(TI40, VI40, int64_t, 2, "i40")

/*
 * TI48
 */

RamenValue *TI48::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VI48(v) : nullptr;
}

UNSERIALIZE(TI48, VI48, int64_t, 2, "i48")

/*
 * TI56
 */

RamenValue *TI56::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VI56(v) : nullptr;
}

UNSERIALIZE(TI56, VI56, int64_t, 2, "i56")

/*
 * TI64
 */

RamenValue *TI64::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VI64(v) : nullptr;
}

UNSERIALIZE(TI64, VI64, int64_t, 2, "i64")

/*
 * TI128
 */

RamenValue *TI128::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VI128(v) : nullptr;
}

UNSERIALIZE(TI128, VI128, int128_t, 4, "i128")

/*
 * User Types
 */

/*
 * TEth
 */

RamenValue *TEth::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VEth(v) : nullptr;
}

UNSERIALIZE(TEth, VEth, uint64_t, 2, "eth")

/*
 * TIpv4
 */

UNSERIALIZE(TIpv4, VIpv4, uint32_t, 1, "ipv4")

/*
 * TIpv6
 */

UNSERIALIZE(TIpv6, VIpv6, uint128_t, 4, "ipv6")

/*
 * TIp
 */

RamenValue *TIp::unserialize(uint32_t const *&start, uint32_t const *max, bool) const
{
  /* TIps are serialized as a tag and a u32 or u128. */

  unsigned const avail = max - start;
  if (avail < 2) {
    qCritical() << "Cannot unserialize a TIp";
    return nullptr;
  }

  switch ((uint8_t)*start) {
    case 0U:  // V4
      {
        VIp *v = new VIp(*(start + 1));
        start += 2;
        return v;
      }
    case 1U:  // V6
      {
        if (avail < 5) {
          qCritical() << "Cannot unserialize a TIp for v6";
          return nullptr;
        }
        uint128_t ip;
        memcpy(&ip, start + 1, sizeof(ip));
        VIp *v = new VIp(ip);
        start += 5;
        return v;
      }
    default:
      qCritical() << "Invalid tag" << *start << "when deserializing a TIp";
      return nullptr;
  }
}

/*
 * TCidrv4
 */

RamenValue *TCidrv4::unserialize(uint32_t const *&start, uint32_t const *max, bool) const
{
  /* Cidrv4 are serialized as an u32 and an u8. */

  unsigned const avail = max - start;
  if (avail < 2) {
    qCritical() << "Cannot unserialize a TCidrv4";
    return nullptr;
  }

  uint32_t const ip = *start;
  uint8_t const mask = *(start + 1);
  start += 2;
  return new VCidrv4(ip, mask);
}

/*
 * TCidrv6
 */

RamenValue *TCidrv6::unserialize(uint32_t const *&start, uint32_t const *max, bool) const
{
  /* Cidrv6 are serialized as an uint128 followed by a u8 */

  unsigned const avail = max - start;
  if (avail < 5) {
    qCritical() << "Cannot unserialize a TCidrv6";
    return nullptr;
  }

  uint128_t ip;
  memcpy(&ip, start, sizeof(ip));
  uint8_t const mask = *(start + 4);
  start += 5;
  return new VCidrv6(ip, mask);
}

/*
 * TCidr
 */

RamenValue *TCidr::unserialize(uint32_t const *&start, uint32_t const *max, bool) const
{
  /* A generic TCidr is encoded with a tag followed by the actual Cidrv4/v6 */

  unsigned const avail = max - start;
  if (avail < 3) {
    qCritical() << "Cannot unserialize a TCidr";
    return nullptr;
  }

  switch ((uint8_t)*start) {
    case 4U:  // V4
      {
        VCidr *v = new VCidr(*(start + 1), (uint8_t)*(start + 2));
        start += 3;
        return v;
      }
    case 6U:  // V6
      {
        if (avail < 6) {
          qCritical() << "Cannot unserialize a TCidr for v6";
          return nullptr;
        }
        uint128_t ip;
        memcpy(&ip, start + 1, sizeof(ip));
        VCidr *v = new VCidr(ip, (uint8_t)*(start + 5));
        start += 6;
        return v;
      }
    default:
      qCritical() << "Invalid tag" << *start << "when deserializing a TCidr";
      return nullptr;
  }
}

/*
 * Compound Types
 */

/*
 * TVec
 */

QString const TVec::toQString() const
{
  return subType->toQString() + QString("[") + QString::number(dim) + QString("]");
}

QString TVec::columnName(int i) const
{
  if ((size_t)i >= dim) return QString();
  return QString("#") + QString::number(i);
}

std::shared_ptr<RamenType const> TVec::columnType(int i) const
{
  if ((size_t)i >= dim) return nullptr;
  return subType;
}

RamenValue *TVec::unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const
{
  unsigned char *nullmask = (unsigned char *)start;
  start += roundUpWords(roundUpBytes(nullmaskWidth(topLevel)));
  if (start > max) {
    qCritical() << "Invalid start/max for vector" << *this;
    return nullptr;
  }

  VVec *vec = new VVec(dim);
  unsigned null_i = 0;
  for (size_t i = 0; i < dim; i++) {
    if (subType->nullable) {
      vec->append(
        bitSet(nullmask, null_i) ?
          subType->vtyp->unserialize(start, max, false) :
          new VNull());
      null_i++;
    } else {
      vec->append(
        subType->vtyp->unserialize(start, max, false));
    }
  }

  return vec;
}

/*
 * TList
 */

QString const TList::toQString() const
{
  return subType->toQString() + QString("[]");
}

size_t TList::nullmaskWidth(bool) const
{
  assert(!"List nullmaskWidth is special!");
}

RamenValue *TList::unserialize(uint32_t const *&start, uint32_t const *max, bool) const
{
  // Like vectors, but preceded with the number of items:
  if (start >= max) {
    qCritical() << "Invalid start/max for list count" << *this;
    return nullptr;
  }

  size_t const dim = *(start++);
  unsigned char *nullmask = (unsigned char *)start;
  start += roundUpWords(roundUpBytes(dim));
  if (start > max) {
    qCritical() << "Invalid start/max for list" << *this;
    return nullptr;
  }

  VLst *lst = new VLst(dim);
  unsigned null_i = 0;
  for (size_t i = 0; i < dim; i++) {
    if (subType->nullable) {
      lst->append(
        bitSet(nullmask, null_i) ?
          subType->vtyp->unserialize(start, max, false) :
          new VNull());
      null_i++;
    } else {
      lst->append(
        subType->vtyp->unserialize(start, max, false));
    }
  }

  return lst;
}

/*
 * TTuple
 */

QString const TTuple::toQString() const
{
  QString ret("(");
  bool needSep = false;
  for (auto const &t : fields) {
    if (needSep) ret += ";";
    else needSep = true;
    ret += t->toQString();
  }
  return ret + QString(")");
}

QString TTuple::columnName(int i) const
{
  if ((size_t)i >= fields.size()) return QString();
  return QString("#") + QString::number(i);
}

std::shared_ptr<RamenType const> TTuple::columnType(int i) const
{
  if ((size_t)i >= fields.size()) return nullptr;
  return fields[i];
}

RamenValue *TTuple::unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const
{
  unsigned char *nullmask = (unsigned char *)start;
  start += roundUpWords(roundUpBytes(nullmaskWidth(topLevel)));
  if (start > max) {
    qCritical() << "Invalid start/max for tuple" << *this;
    return nullptr;
  }

  VTuple *tuple = new VTuple(fields.size());
  unsigned null_i = 0;
  // Notice that despite the nullmask is large enough for all items, we
  // only increment null_i when the field is indeed nullable:
  for (auto &subType : fields) {
    if (subType->nullable) {
      tuple->append(
        bitSet(nullmask, null_i) ?
          subType->vtyp->unserialize(start, max, false) :
          new VNull());
      null_i++;
    } else {
      tuple->append(
        subType->vtyp->unserialize(start, max, false));
    }
  }
  return tuple;
}

/*
 * TRecord
 */

void TRecord::append(QString const name, std::shared_ptr<RamenType const> type)
{
  serOrder.push_back(fields.size());
  fields.emplace_back(name, type);
  // Sort serOrder again after each push:
  std::sort(serOrder.begin(), serOrder.end(), [this](uint16_t a, uint16_t b) {
    return fields[a].first < fields[b].first;
  });
}

QString const TRecord::toQString() const
{
  QString ret("(");
  bool needSep = false;
  for (auto const &p : fields) {
    if (needSep) ret += ";";
    else needSep = true;
    ret += p.first + QString(":") + p.second->toQString();
  }
  return ret + QString(")");
}

QString TRecord::columnName(int i) const
{
  if ((size_t)i >= fields.size()) return QString();
  return fields[i].first;
}

std::shared_ptr<RamenType const> TRecord::columnType(int i) const
{
  if ((size_t)i >= fields.size()) return nullptr;
  return fields[i].second;
}

size_t TRecord::nullmaskWidth(bool topLevel) const
{
  /* TODO: fix nullmask for compound types (ie: reserve a bit only to
   *       nullable subfields) */
  if (topLevel) {
    size_t w = 0;
    for (auto &f : fields) {
      if (f.second->nullable) w++;
    }
    if (verbose)
      qDebug() << "top-level nullmask has" << w << "bits";
    return w;
  } else {
    return fields.size();
  }
}

RamenValue *TRecord::unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const
{
  if (verbose)
    qDebug() << "Start to unserialize a record"
              << (topLevel ? " (top-level)":"");
  unsigned char *nullmask = (unsigned char *)start;
  start += roundUpWords(roundUpBytes(nullmaskWidth(topLevel)));
  if (start > max) {
    qCritical() << "Invalid start/max for record" << *this;
    return nullptr;
  }

  size_t const numFields = fields.size();
  VRecord *rec = new VRecord(fields.size());
  unsigned null_i = 0;
  assert(serOrder.size() == numFields);
  for (unsigned i = 0; i < numFields; i++) {
    size_t const fieldIdx = serOrder[i];
    QString const &fieldName = fields[ fieldIdx ].first;
    std::shared_ptr<RamenType const> subType = fields[ fieldIdx ].second;
    if (verbose)
      qDebug() << "Next field is" << fieldName << ","
               << (subType->nullable ?
                    (bitSet(nullmask, null_i) ?
                      "not null" : "null") :
                    "not nullable");
    rec->set(
      fieldIdx, fieldName,
      subType->nullable && !bitSet(nullmask, null_i) ?
        new VNull() :
        subType->vtyp->unserialize(start, max, false));
  }

  return rec;
}

/*
 * TSum
 */

void TSum::append(QString const name, std::shared_ptr<RamenType const> type)
{
  alternatives.emplace_back(name, type);
}

QString const TSum::toQString() const
{
  QString ret("(");
  bool needSep = false;
  for (auto const &p : alternatives) {
    if (needSep) ret += "|";
    else needSep = true;
    ret += p.first + QString(" ") + p.second->toQString();
  }
  return ret + QString(")");
}

RamenValue *TSum::unserialize(uint32_t const *&start, uint32_t const *max, bool) const
{
  if (verbose)
    qDebug() << "Start to unserialize a sum";

  if (max - start < 1) {
    qCritical() << "Invalid start/max for sum" << *this;
    return nullptr;
  }

  uint16_t const nullmask = ((uint16_t *)start)[0];
  bool const nullbit = (nullmask & 1) == 1;
  uint16_t const label = ((uint16_t *)start)[1];

  if (label >= alternatives.size()) {
    qCritical() << "Invalid label (" << label << ") for sum types with only "
                << alternatives.size() << " constructors";
    return nullptr;
  }

  start += roundUpWords(2 * sizeof(uint16_t));

  QString const &cstrName = alternatives[ label ].first;
  std::shared_ptr<RamenType const> subType = alternatives[ label ].second;
  if (verbose)
    qDebug() << "Constructor is" << cstrName << ","
             << (subType->nullable ?
                  (nullbit ?  "not null" : "null") :
                  "not nullable");

  VSum *sum = new VSum(label, cstrName,
    subType->nullable && !nullbit ?
      new VNull() :
      subType->vtyp->unserialize(start, max, false));

  return sum;
}

/*
 * Misc
 */

// The only non-blocky value type is the dreadful Unknown:
// Does not alloc on OCaml heap
static DessserValueType *intyValueTypeOfOCaml(value v_)
{
  assert(Long_val(v_) == 0); // Unknown
  return new Unknown;
}

static DessserValueType *MacTypeOfOCaml(value v_)
{
  DessserValueType *ret;
  switch (Long_val(v_)) {
    case 0: ret = new TFloat; break;
    case 1: ret = new TString; break;
    case 2: ret = new TBool; break;
    case 3: ret = new TChar; break;
    case 4: ret = new TU8; break;
    case 5: ret = new TU16; break;
    case 6: ret = new TU24; break;
    case 7: ret = new TU32; break;
    case 8: ret = new TU40; break;
    case 9: ret = new TU48; break;
    case 10: ret = new TU56; break;
    case 11: ret = new TU64; break;
    case 12: ret = new TU128; break;
    case 13: ret = new TI8; break;
    case 14: ret = new TI16; break;
    case 15: ret = new TI24; break;
    case 16: ret = new TI32; break;
    case 17: ret = new TI40; break;
    case 18: ret = new TI48; break;
    case 19: ret = new TI56; break;
    case 20: ret = new TI64; break;
    case 21: ret = new TI128; break;
    default:
      assert(!"Unknown tag for mac_type!");
  }
  return ret;
}

static DessserValueType *WellknownUserTypeOfOCaml(value v_)
{
  assert(Is_block(v_));
  assert(Wosize_val(v_) == 2);
  QString const name(String_val(Field(v_, 0)));

  if (name == "Eth") {
    return new TEth;
  } else if (name == "Ip4") {
    return new TIpv4;
  } else if (name == "Ip6") {
    return new TIpv6;
  } else if (name == "Ip") {
    return new TIp;
  } else if (name == "Cidr4") {
    return new TCidrv4;
  } else if (name == "Cidr6") {
    return new TCidrv6;
  } else if (name == "Cidr") {
    return new TCidr;
  } else {
    qCritical() << "Unknown user_type " << name;
    assert(!"Unknown user_type");
  }
}

static DessserValueType *blockyValueTypeOfOCaml(value v_)
{
  DessserValueType *ret;

  switch (Tag_val(v_)) {
    case 0:  // Mac of mac_type
      ret = MacTypeOfOCaml(Field(v_, 0));
      break;
    case 1:  // Usr of user_type
      ret = WellknownUserTypeOfOCaml(Field(v_, 0));
      break;
    case 2:  // TVec of int * maybe_nullable
      {
        std::shared_ptr<RamenType const> subType =
          std::make_shared<RamenType const>(Field(v_, 1));
        ret = new TVec(Long_val(Field(v_, 0)), subType);
      }
      break;
    case 3:  // TList of maybe_nullable
      {
        std::shared_ptr<RamenType const> subType =
          std::make_shared<RamenType const>(Field(v_, 0));
        ret = new TList(subType);
      }
      break;
    case 4:  // TTup of maybe_nullable array
      {
        value tmp_ = Field(v_, 0);
        assert(Is_block(tmp_));  // an array of types
        unsigned const numFields = Wosize_val(tmp_);
        TTuple *tuple = new TTuple(numFields);
        for (unsigned f = 0; f < numFields; f++) {
          std::shared_ptr<RamenType const> field =
            std::make_shared<RamenType const>(Field(tmp_, f));
          tuple->append(field);
        }
        ret = tuple;
      }
      break;
    case 5:  // TRec of (string * maybe_nullable) array
      {
        value tmp_ = Field(v_, 0);
        assert(Is_block(tmp_));  // an array of name * type pairs

        unsigned const numFields = Wosize_val(tmp_);
        TRecord *rec = new TRecord(numFields);
        for (unsigned f = 0; f < numFields; f++) {
          v_ = Field(tmp_, f);
          assert(Is_block(v_));   // a pair of string * type
          std::shared_ptr<RamenType const> subType =
            std::make_shared<RamenType const>(Field(v_, 1));
          rec->append(QString(String_val(Field(v_, 0))), subType);
        }
        ret = rec;
      }
      break;
    case 6:  // TSum of (string * maybe_nullable) array
      {
        value tmp_ = Field(v_, 0);
        assert(Is_block(tmp_));  // an array of name * type pairs

        unsigned const numCstrs = Wosize_val(tmp_);
        TSum *sum = new TSum(numCstrs);
        for (unsigned f = 0; f < numCstrs; f++) {
          v_ = Field(tmp_, f);
          assert(Is_block(v_));   // a pair of string * type
          std::shared_ptr<RamenType const> subType =
            std::make_shared<RamenType const>(Field(v_, 1));
          sum->append(QString(String_val(Field(v_, 0))), subType);
        }
        ret = sum;
      }
      break;
    default:
      assert(!"Unknown tag for compound DessserValueType!");
  }
  return ret;
}

// Does not alloc on OCaml heap
DessserValueType *DessserValueType::ofOCaml(value v_)
{
  return
    Is_block(v_) ? blockyValueTypeOfOCaml(v_) : intyValueTypeOfOCaml(v_);
}
