#ifndef SITEITEM_H_190509
#define SITEITEM_H_190509
#include <optional>
#include <vector>
#include "OperationsItem.h"

class GraphViewSettings;

class SiteItem : public OperationsItem
{
protected:
  void addLabels(std::vector<std::pair<QString const, QString const>> *) const;
public:
  std::optional<bool> isMaster;
  std::vector<ProgramItem *> programs;
  SiteItem(OperationsItem *treeParent, QString const &name, GraphViewSettings const *, unsigned paletteSize);
  ~SiteItem();
  QVariant data(int) const;
  void reorder(OperationsModel const *);
  QRectF operationRect() const;
};

std::ostream &operator<<(std::ostream &, SiteItem const &);

#endif
