#ifndef FUNCTIONITEM_H_190509
#define FUNCTIONITEM_H_190509
#include <optional>
#include <memory>
#include <vector>
#include "confKey.h"
#include "confValue.h"
#include "GraphItem.h"

class GraphViewSettings;
class TailModel;

class FunctionItem : public GraphItem
{
  Q_OBJECT

protected:
  std::vector<std::pair<QString const, QString const>> labels() const;

public:
  // tuples owned by this object:
  std::vector<RamenValue const *> tuples;
  std::shared_ptr<conf::Worker const> worker;
  std::shared_ptr<conf::RuntimeStats const> runtimeStats;
  std::shared_ptr<conf::TimeRange const> archivedTimes;
  std::optional<int64_t> numArcFiles;
  std::optional<int64_t> numArcBytes;
  std::optional<int64_t> allocArcBytes;

  unsigned channel; // could also be used to select a color?
  // FIXME: Function destructor must clean those:
  std::vector<FunctionItem const *> parents;

  FunctionItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *);
  ~FunctionItem();
  int columnCount() const;
  QVariant data(int, int) const;
  QRectF operationRect() const;

  // Returns nullptr if the info is not available yet
  CompiledFunctionInfo const *compiledInfo() const;
  // Returns nullptr is the type is still unknown:
  std::shared_ptr<RamenType const> outType() const;
  int numRows() const;
  int numColumns() const;
  RamenValue const *tupleData(int row, int column) const;
  QString header(unsigned) const;
  bool isTopHalf() const;

  TailModel *tailModel; // created only on demand

private slots:
  void addTuple(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void beginAddTuple(QModelIndex const &, int first, int last);
  void endAddTuple();
};

std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
