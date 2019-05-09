#ifndef FUNCTIONITEM_H_190509
#define FUNCTIONITEM_H_190509
#include <optional>
#include <vector>
#include "OperationsItem.h"

class FunctionItem : public OperationsItem
{
  /* The conf sync will /add/del/update parents indexed so it's easy to
   * save them in a vector, as long as we can skip values.
   * But the conf sync does not guarantee that parents will be created
   * before they are referenced, so here we have this lazy reference
   * system of either refering to a parent by name (not yet actually needed)
   * or by pointer (cached). */

public:
  std::optional<bool> isUsed;
  // FIXME: Function destructor must clean those:
  // FIXME: is this actually needed to know the parents?
  std::vector<FunctionItem const*> parents;
  FunctionItem(OperationsItem *parent, QString const &name);
  ~FunctionItem();
  QVariant data(int) const;
};

std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
