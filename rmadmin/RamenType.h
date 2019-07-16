#ifndef RAMENTYPE_H_190716
#define RAMENTYPE_H_190716
/* A RamenType represents a RamenTypes.t.
 * This is useful to deserialize tuples.
 * Also, maybe in the future we could have confValues of that type. */
#include <QCoreApplication>
extern "C" {
# include <caml/mlvalues.h>
}
#include "RamenTypeStructure.h"
#include "serValue.h"

struct RamenType
{
  RamenTypeStructure structure;
  bool nullable;

  RamenType(RamenTypeStructure, bool);
  RamenType() : RamenType(EmptyType, false) {}

  virtual ~RamenType() {}

  virtual QString structureToQString() const = 0;
  QString toQString() const
  {
    QString s(structureToQString());
    if (nullable) s.append("?");
    return s;
  }
  value toOCamlValue() const;
  virtual unsigned numColumns() const { return 0; };
  virtual QString columnName(unsigned) const = 0;
  virtual std::shared_ptr<RamenType const> columnType(unsigned) const = 0;
  // Return the size of the required nullmask in _bits_
  // ie. number of nullable subfields.
  virtual size_t nullmaskWidth(bool) const { return 0; }
  virtual bool isNumeric() const = 0;
};

struct RamenTypeScalar : public RamenType
{
  RamenTypeScalar(RamenTypeStructure t, bool n) : RamenType(t, n) {}
  QString structureToQString() const;
  unsigned numColumns() const { return 1; }
  QString columnName(unsigned) const;
  std::shared_ptr<RamenType const> columnType(unsigned) const { return nullptr; }
  bool isNumeric() const;
};

struct RamenTypeTuple : public RamenType
{
  // For Tuples and other composed types, subTypes are owned by their parent
  std::vector<std::shared_ptr<RamenType const>> fields;
  RamenTypeTuple(std::vector<std::shared_ptr<RamenType const>> fields_, bool n) :
    RamenType(TupleType, n), fields(fields_) {}
  QString structureToQString() const;
  // Maybe displaying a tuple in a single column would be preferable?
  unsigned numColumns() const { return fields.size(); }
  QString columnName(unsigned i) const
  {
    if (i >= fields.size()) return QString();
    return QString("#") + QString::number(i);
  }
  std::shared_ptr<RamenType const> columnType(unsigned i) const
  {
    if (i >= fields.size()) return nullptr;
    return fields[i];
  }
  size_t nullmaskWidth(bool) const { return fields.size(); }
  bool isNumeric() const { return false; }
};

struct RamenTypeVec : public RamenType
{
  unsigned dim;
  std::shared_ptr<RamenType const> subType;
  RamenTypeVec(unsigned dim_, std::shared_ptr<RamenType const> subType_, bool n) :
    RamenType(VecType, n), dim(dim_), subType(subType_) {}
  QString structureToQString() const;
  // Maybe displaying a vector in a single column would be preferable?
  unsigned numColumns() const { return dim; }
  QString columnName(unsigned i) const
  {
    if (i >= dim) return QString();
    return QString("#") + QString::number(i);
  }
  std::shared_ptr<RamenType const> columnType(unsigned i) const
  {
    if (i >= dim) return nullptr;
    return subType;
  }
  size_t nullmaskWidth(bool) const { return dim; }
  bool isNumeric() const { return false; }
};

struct RamenTypeList : public RamenType
{
  std::shared_ptr<RamenType const> subType;

  RamenTypeList(std::shared_ptr<RamenType const> subType_, bool n) :
    RamenType(ListType, n), subType(subType_) {}

  QString structureToQString() const;
  // Lists are displayed in a single column as they have a variable length
  unsigned numColumns() const { return 1; }
  QString columnName(unsigned i) const
  {
    if (i != 0) return QString();
    return QString(QCoreApplication::translate("QMainWindow", "list"));
  }
  std::shared_ptr<RamenType const> columnType(unsigned i) const
  {
    if (i != 0) return nullptr;
    return std::shared_ptr<RamenType const>(new RamenTypeList(subType, nullable));;
  }
  size_t nullmaskWidth(bool) const
  {
    assert(!"List nullmaskWidth is special!");
  }
  bool isNumeric() const { return false; }
};

// This is the interesting one:
struct RamenTypeRecord : public RamenType
{
  // Fields are ordered in serialization order:
  std::vector<std::pair<QString, std::shared_ptr<RamenType const>>> fields;
  // Here is the serialization order:
  std::vector<uint16_t> serOrder;

  RamenTypeRecord(std::vector<std::pair<QString, std::shared_ptr<RamenType const>>> fields_, bool n);
  QString structureToQString() const;
  unsigned numColumns() const { return fields.size(); }
  QString columnName(unsigned i) const
  {
    if (i >= fields.size()) return QString();
    return fields[i].first;
  }
  std::shared_ptr<RamenType const> columnType(unsigned i) const
  {
    if (i >= fields.size()) return nullptr;
    return fields[i].second;
  }
  size_t nullmaskWidth(bool topLevel) const
  {
    // TODO: fix nullmask for compound types (ie: reserve a bit only to
    //       nullable subfields)
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
  bool isNumeric() const { return false; }
};

RamenType *ramenTypeOfOCaml(value v_);
std::ostream &operator<<(std::ostream &, RamenType const &);

#endif
