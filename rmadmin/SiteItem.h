#ifndef SITEITEM_H_190509
#define SITEITEM_H_190509
#include <optional>
#include <vector>
#include "OperationsItem.h"

class SiteItem : public OperationsItem
{
public:
  std::optional<bool> isMaster;
  std::vector<ProgramItem *> programs;
  SiteItem(OperationsItem *treeParent, QString const &name);
  ~SiteItem();
  QVariant data(int) const;
  void reorder(OperationsModel const *);
};

std::ostream &operator<<(std::ostream &, SiteItem const &);

#endif
