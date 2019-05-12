#ifndef FUNCTIONITEM_H_190509
#define FUNCTIONITEM_H_190509
#include <optional>
#include <vector>
#include "OperationsItem.h"

class GraphViewSettings;

class FunctionItem : public OperationsItem
{
public:
  std::optional<bool> isUsed;
  unsigned channel; // could also be used to select a color?
  // FIXME: Function destructor must clean those:
  std::vector<FunctionItem const*> parents;
  FunctionItem(OperationsItem *treeParent, QString const &name, GraphViewSettings const *, unsigned paletteSize);
  ~FunctionItem();
  QVariant data(int) const;
  QRectF operationRect() const;
};

std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
