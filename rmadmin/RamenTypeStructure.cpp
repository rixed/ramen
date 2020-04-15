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
#include "RamenTypeStructure.h"

static bool const verbose(false);

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

value RamenTypeStructure::toOCamlValue() const
{
  qCritical() << "Unimplemented conversion to OCaml value from"
              << toQString();
  assert(!"Don't know how to convert from a RamenTypeStructure");
}

/*
 * TEmpty
 */

RamenValue *TEmpty::unserialize(uint32_t const *&, uint32_t const *, bool) const
{
  return new VNull();
}

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
 * TNum
 */

RamenValue *TNum::unserialize(uint32_t const *&, uint32_t const *, bool) const
{
  assert("!Cannot unserialize untyped TNum");
  return nullptr;
}

/*
 * TAny
 */

RamenValue *TAny::unserialize(uint32_t const *&, uint32_t const *, bool) const
{
  assert("!Cannot unserialize untyped TAny");
  return nullptr;
}

/*
 * TU8
 */

RamenValue *TU8::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VU8(v) : nullptr;
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

UNSERIALIZE(TU8, VU8, uint8_t, 1, "u8")

/*
 * TChar
 */

RamenValue *TChar::valueOfQString(QString const s) const
{
  bool ok = true;
  long v = s.toLong(&ok);
  return ok ? new VChar(v) : nullptr;
}

UNSERIALIZE(TChar, VChar, char, 1, "char")

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
          subType->structure->unserialize(start, max, false) :
          new VNull());
      null_i++;
    } else {
      tuple->append(
        subType->structure->unserialize(start, max, false));
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
              << (topLevel ? "(top-level)":"");
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
    if (subType->nullable) {
      rec->set(
        fieldIdx, fieldName,
        bitSet(nullmask, null_i) ?
          subType->structure->unserialize(start, max, false) :
          new VNull());
      null_i++;
    } else {
      rec->set(
        fieldIdx, fieldName,
        subType->structure->unserialize(start, max, false));
    }
  }

  return rec;
}

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
          subType->structure->unserialize(start, max, false) :
          new VNull());
      null_i++;
    } else {
      vec->append(
        subType->structure->unserialize(start, max, false));
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
  // Like vectors, but preceeded with the number of items:
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

  VList *lst = new VList(dim);
  unsigned null_i = 0;
  for (size_t i = 0; i < dim; i++) {
    if (subType->nullable) {
      lst->append(
        bitSet(nullmask, null_i) ?
          subType->structure->unserialize(start, max, false) :
          new VNull());
      null_i++;
    } else {
      lst->append(
        subType->structure->unserialize(start, max, false));
    }
  }

  return lst;
}

/*
 * Misc
 */

// Does not alloc on OCaml heap
static RamenTypeStructure *scalarStructureOfOCaml(value v_)
{
  RamenTypeStructure *ret;
  switch (Long_val(v_)) {
    case 0: ret = new TEmpty; break;
    case 1: ret = new TFloat; break;
    case 2: ret = new TString; break;
    case 3: ret = new TBool; break;
    case 4: ret = new TChar; break;
    case 5: ret = new TNum; break;
    case 6: ret = new TAny; break;
    case 7: ret = new TU8; break;
    case 8: ret = new TU16; break;
    case 9: ret = new TU32; break;
    case 10: ret = new TU64; break;
    case 11: ret = new TU128; break;
    case 12: ret = new TI8; break;
    case 13: ret = new TI16; break;
    case 14: ret = new TI32; break;
    case 15: ret = new TI64; break;
    case 16: ret = new TI128; break;
    case 17: ret = new TEth; break;
    case 18: ret = new TIpv4; break;
    case 19: ret = new TIpv6; break;
    case 20: ret = new TIp; break;
    case 21: ret = new TCidrv4; break;
    case 22: ret = new TCidrv6; break;
    case 23: ret = new TCidr; break;
    default:
      assert(!"Unknown tag for scalar RamenTypeStructure!");
  }
  return ret;
}

static RamenTypeStructure *blockyStructureOfOCaml(value v_)
{
  RamenTypeStructure *ret;
  switch (Tag_val(v_)) {
    case 0:
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
    case 1:
      {
        std::shared_ptr<RamenType const> subType =
          std::make_shared<RamenType const>(Field(v_, 1));
        ret = new TVec(Long_val(Field(v_, 0)), subType);
      }
      break;
    case 2:
      {
        std::shared_ptr<RamenType const> subType =
          std::make_shared<RamenType const>(Field(v_, 0));
        ret = new TList(subType);
      }
      break;
    case 3:
      {
        value tmp_ = Field(v_, 0);
        assert(Is_block(tmp_));  // an array of name * type pairs

        std::vector<std::pair<QString, std::shared_ptr<RamenType const>>> fields;

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
    default:
      assert(!"Unknown tag for compound RamenTypeStructure!");
  }
  return ret;
}

// Does not alloc on OCaml heap
RamenTypeStructure *RamenTypeStructure::ofOCaml(value v_)
{
  return
    Is_block(v_) ? blockyStructureOfOCaml(v_) : scalarStructureOfOCaml(v_);
}
