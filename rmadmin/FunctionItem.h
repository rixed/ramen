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
  std::vector<ser::Value const *> tuples;
  std::shared_ptr<conf::Worker const> worker;
  std::optional<double> firstStartupTime;
  std::optional<double> lastStartupTime;
  std::optional<double> eventTimeMin;
  std::optional<double> eventTimeMax;
  std::optional<int64_t> totalTuples;
  std::optional<int64_t> totalBytes;
  std::optional<double> totalCpu;
  std::optional<int64_t> maxRAM;
  std::optional<int64_t> numArcFiles;
  std::optional<int64_t> numArcBytes;
  std::optional<int64_t> allocArcBytes;

  unsigned channel; // could also be used to select a color?
  // FIXME: Function destructor must clean those:
  std::vector<FunctionItem const *> parents;
  FunctionItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *);
  ~FunctionItem();
  QVariant data(int) const;
  QRectF operationRect() const;

  conf::Key functionKey(std::string perFuncKey) const;

  std::shared_ptr<conf::RamenType const> outType() const;
  int numRows() const;
  int numColumns() const;
  ser::Value const *tupleData(int row, int column) const;
  QString header(unsigned) const;

  TailModel *tailModel; // created only on demand

private slots:
  void addTuple(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void beginAddTuple(QModelIndex const &, int first, int last);
  void endAddTuple();
};

std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
