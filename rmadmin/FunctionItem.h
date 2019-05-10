#ifndef FUNCTIONITEM_H_190509
#define FUNCTIONITEM_H_190509
#include <optional>
#include <vector>
#include "OperationsItem.h"

class FunctionItem : public OperationsItem
{
protected:
  std::vector<std::pair<QString const, QString const>> graphLabels() const;

public:
  std::optional<bool> isUsed;
  // FIXME: Function destructor must clean those:
  // FIXME: is this actually needed to know the parents?
  std::vector<FunctionItem const*> parents;
  FunctionItem(OperationsItem *treeParent, QString const &name);
  ~FunctionItem();
  QVariant data(int) const;
};

std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
