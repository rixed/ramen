#include <iostream>
#include <cassert>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
}
#include "misc.h"
#include "RamenType.h"
#include "RamenValue.h"
#include "RamenTypeStructure.h"

static bool verbose = false;

// Returns the number of words required to store that many bytes:
static size_t roundUpWords(size_t sz)
{
  return (sz + 3) >> 2;
}

// TODO: an actual object with an end to check against
static bool bitSet(unsigned char const *nullmask, unsigned null_i)
{
  if (null_i >= 8) return bitSet(nullmask + 1, null_i - 8);
  else return (*nullmask) & (1 << null_i);
}

value RamenTypeStructure::toOCamlValue() const
{
  std::cerr << "Unimplemented conversion to OCaml value from "
            << toQString().toStdString() << std::endl;
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
    std::cerr << "Cannot unserialize a float" << std::endl;
    return nullptr;
  }

  double v;
  static_assert(sizeof(double) <= 2 * sizeof(uint32_t));
  memcpy(&v, start, sizeof(v));
  if (verbose) std::cout << "float = " << v << std::endl;
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
    std::cerr << "Cannot unserialize a string" << std::endl;
    return nullptr;
  }

  size_t const len = *(start++);
  size_t const wordLen = roundUpWords(len);
  if (start + wordLen > max) {
    std::cerr << "Cannot unserialize of length " << len << std::endl;
    return nullptr;
  }

  QString v;
  char const *c = (char const *)start;
  for (size_t i = 0; i < len; i++) {
    v.append(QChar(c[i]));
  }
  if (verbose) std::cout << "string = " << v.toStdString() << std::endl;
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
    std::cerr << "Cannot unserialize a bool" << std::endl;
    return nullptr;
  }

  bool const v(!! *start);
  if (verbose) std::cout << "bool = " << v << std::endl;
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
      std::cerr << "Cannot unserialize a "# name << std::endl; \
      return nullptr; \
    } \
  \
    TC const v = *(TC const *)start; \
    if (verbose) std::cout << #name " = " << v << std::endl; \
    start += words; \
  \
    return new TV(v); \
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

RamenValue *TIp::unserialize(uint32_t const *&, uint32_t const *, bool) const
{
  assert(!"Unimplemented TIp::unserialize");
}

/*
 * TCidrv4
 */

RamenValue *TCidrv4::unserialize(uint32_t const *&, uint32_t const *, bool) const
{
  assert(!"Unimplemented TCidrv4::unserialize");
}

/*
 * TCidrv6
 */

RamenValue *TCidrv6::unserialize(uint32_t const *&, uint32_t const *, bool) const
{
  assert(!"Unimplemented TCidrv6::unserialize");
}

/*
 * TCidr
 */

RamenValue *TCidr::unserialize(uint32_t const *&, uint32_t const *, bool) const
{
  assert(!"Unimplemented TCidr::unserialize");
}

/*
 * TTuple
 */

QString const TTuple::toQString() const
{
  QString ret("(");
  bool needSep = false;
  for (auto t : fields) {
    if (needSep) ret += ";";
    else needSep = true;
    ret += t->toQString();
  }
  return ret + QString(")");
}

QString TTuple::columnName(unsigned i) const
{
  if (i >= fields.size()) return QString();
  return QString("#") + QString::number(i);
}

std::shared_ptr<RamenType const> TTuple::columnType(unsigned i) const
{
  if (i >= fields.size()) return nullptr;
  return fields[i];
}

RamenValue *TTuple::unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const
{
  unsigned char *nullmask = (unsigned char *)start;
  start += roundUpWords(nullmaskWidth(topLevel));
  if (start > max) {
    std::cerr << "Invalid start/max for tuple " << *this << std::endl;
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
  for (auto p : fields) {
    if (needSep) ret += ";";
    else needSep = true;
    ret += p.first + QString(":") + p.second->toQString();
  }
  return ret + QString(")");
}

QString TRecord::columnName(unsigned i) const
{
  if (i >= fields.size()) return QString();
  return fields[i].first;
}

std::shared_ptr<RamenType const> TRecord::columnType(unsigned i) const
{
  if (i >= fields.size()) return nullptr;
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
    return w;
  } else {
    return fields.size();
  }
}

RamenValue *TRecord::unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const
{
  unsigned char *nullmask = (unsigned char *)start;
  start += roundUpWords(nullmaskWidth(topLevel));
  if (start > max) {
    std::cerr << "Invalid start/max for record " << *this << std::endl;
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
    if (verbose) {
      std::cout << "Next field is " << fieldName.toStdString() << ", "
                << (subType->nullable ?
                     (bitSet(nullmask, null_i) ?
                       "not null" : "null") :
                     "not nullable") << std::endl;
    }
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

QString TVec::columnName(unsigned i) const
{
  if (i >= dim) return QString();
  return QString("#") + QString::number(i);
}

std::shared_ptr<RamenType const> TVec::columnType(unsigned i) const
{
  if (i >= dim) return nullptr;
  return subType;
}

RamenValue *TVec::unserialize(uint32_t const *&start, uint32_t const *max, bool topLevel) const
{
  unsigned char *nullmask = (unsigned char *)start;
  start += roundUpWords(nullmaskWidth(topLevel));
  if (start > max) {
    std::cerr << "Invalid start/max for vector " << *this << std::endl;
    return nullptr;
  }

  VVec *vec = new VVec(dim);
  unsigned null_i = 0;
  for (unsigned i = 0; i < dim; i++) {
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
    std::cerr << "Invalid start/max for list count " << *this << std::endl;
    return nullptr;
  }

  size_t const dim = *(start++);
  unsigned char *nullmask = (unsigned char *)start;
  start += roundUpWords(dim);
  if (start > max) {
    std::cerr << "Invalid start/max for list " << *this << std::endl;
    return nullptr;
  }

  VList *lst = new VList(dim);
  unsigned null_i = 0;
  for (unsigned i = 0; i < dim; i++) {
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
    case 4: ret = new TNum; break;
    case 5: ret = new TAny; break;
    case 6: ret = new TU8; break;
    case 7: ret = new TU16; break;
    case 8: ret = new TU32; break;
    case 9: ret = new TU64; break;
    case 10: ret = new TU128; break;
    case 11: ret = new TI8; break;
    case 12: ret = new TI16; break;
    case 13: ret = new TI32; break;
    case 14: ret = new TI64; break;
    case 15: ret = new TI128; break;
    case 16: ret = new TEth; break;
    case 17: ret = new TIpv4; break;
    case 18: ret = new TIpv6; break;
    case 19: ret = new TIp; break;
    case 20: ret = new TCidrv4; break;
    case 21: ret = new TCidrv6; break;
    case 22: ret = new TCidr; break;
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
          std::shared_ptr<RamenType const> field(RamenType::ofOCaml(Field(tmp_, f)));
          tuple->append(field);
        }
        ret = tuple;
      }
      break;
    case 1:
      {
        std::shared_ptr<RamenType const> subType(RamenType::ofOCaml(Field(v_, 1)));
        ret = new TVec(Long_val(Field(v_, 0)), subType);
      }
      break;
    case 2:
      {
        std::shared_ptr<RamenType const> subType(RamenType::ofOCaml(Field(v_, 0)));
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
          std::shared_ptr<RamenType const> subType(RamenType::ofOCaml(Field(v_, 1)));
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

std::ostream &operator<<(std::ostream &os, RamenTypeStructure const &s)
{
  os << s.toQString().toStdString();
  return os;
}
