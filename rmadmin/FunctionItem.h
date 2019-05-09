#ifndef FUNCTIONITEM_H_190509
#define FUNCTIONITEM_H_190509
#include <optional>
#include <vector>
#include "LazyRef.h"
#include "confValue.h"
#include "OperationsItem.h"

class FunctionItem : public OperationsItem
{
  std::optional<bool> isUsed;
  /* The conf sync will /add/del/update parents indexed so it's easy to
   * save them in a vector, as long as we can skip values.
   * But the conf sync does not guarantee that parents will be created
   * before they are referenced, so here we have this lazy reference
   * system of either refering to a parent by name (not yet actually needed)
   * or by pointer (cached). */
  std::vector<LazyRef<conf::Worker, FunctionItem>> parents;
public:
  QString name;
  FunctionItem(OperationsItem *parent, QString name);
  ~FunctionItem();
  QVariant data(int) const;
  void setProperty(QString const &, std::shared_ptr<conf::Value const>);
};

std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
