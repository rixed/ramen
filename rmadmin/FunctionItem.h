#ifndef FUNCTIONITEM_H_190509
#define FUNCTIONITEM_H_190509
#include <optional>
#include <memory>
#include <vector>
#include <QObject>
#include <QString>
#include "confKey.h"
#include "confValue.h"
#include "GraphItem.h"

class GraphViewSettings;
class TailModel;

class Function : public QObject, public GraphData
{
  Q_OBJECT

public:
  /* In addition to the name we want the fqName to be available
   * when al we have is a shared_ptr<Function>: */
  QString const fqName;

  // tuples owned by this object:
  std::vector<std::unique_ptr<RamenValue const>> tuples;
  std::shared_ptr<conf::Worker const> worker;
  std::shared_ptr<conf::RuntimeStats const> runtimeStats;
  std::shared_ptr<conf::TimeRange const> archivedTimes;
  std::optional<int64_t> numArcFiles;
  std::optional<int64_t> numArcBytes;
  std::optional<int64_t> allocArcBytes;

  TailModel *tailModel; // created only on demand

  Function(QString const &name_, QString const &fqName_) :
    GraphData(name_),
    fqName(fqName_),
    tailModel(nullptr)
  {
    tuples.reserve(1000);
  }

  ~Function();

  // Returns nullptr if the info is not available yet
  CompiledFunctionInfo const *compiledInfo() const;
  // Returns nullptr is the type is still unknown:
  std::shared_ptr<RamenType const> outType() const;

  int numRows() const { return tuples.size(); }

  int numColumns() const;
  // Returned value owned by FunctionItem:
  RamenValue const *tupleData(int row, int column) const {
    return row < numRows() ?
      tuples[row]->columnValue(column) : nullptr;
  }

  QString header(unsigned) const;

signals:
  void beginAddTuple(QModelIndex const &, int first, int last);
  void endAddTuple();
};

class FunctionItem : public GraphItem
{
  Q_OBJECT

protected:
  std::vector<std::pair<QString const, QString const>> labels() const;

public:
  // FIXME: Function destructor must clean those:
  // Not the parent in the GraphModel but the parents of the operation:
  std::vector<FunctionItem const *> parents;

  unsigned channel; // could also be used to select a color?

  FunctionItem(
    GraphItem *treeParent, std::unique_ptr<Function>, GraphViewSettings const *);

  int columnCount() const;
  QVariant data(int, int) const;
  QRectF operationRect() const;

  bool isTopHalf() const;
  bool isWorking() const;

private slots:
  void addTuple(conf::Key const &, std::shared_ptr<conf::Value const>);
};

std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
