#ifndef FUNCTIONITEM_H_190509
#define FUNCTIONITEM_H_190509
#include <optional>
#include <vector>
#include "GraphItem.h"

class GraphViewSettings;

class FunctionItem : public GraphItem
{
protected:
  std::vector<std::pair<QString const, QString const>> labels() const;
public:
  std::optional<bool> isUsed;
  std::optional<double> startupTime;
  std::optional<double> eventTimeMin;
  std::optional<double> eventTimeMax;
  std::optional<int64_t> totalTuples;
  std::optional<int64_t> totalBytes;
  std::optional<double> totalCpu;
  std::optional<int64_t> maxRAM;

  unsigned channel; // could also be used to select a color?
  // FIXME: Function destructor must clean those:
  std::vector<FunctionItem const*> parents;
  FunctionItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *, unsigned paletteSize);
  ~FunctionItem();
  QVariant data(int) const;
  QRectF operationRect() const;
};

std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
