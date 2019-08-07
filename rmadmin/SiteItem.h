#ifndef SITEITEM_H_190509
#define SITEITEM_H_190509
#include <optional>
#include <vector>
#include "GraphItem.h"

class GraphViewSettings;

class SiteItem : public GraphItem
{
protected:
  std::vector<std::pair<QString const, QString const>> labels() const;
public:
  std::optional<bool> isMaster;
  std::vector<ProgramItem *> programs;
  SiteItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *);
  ~SiteItem();
  void reorder(GraphModel const *);
  QRectF operationRect() const;
  bool isTopHalf() const { return false; }
};

std::ostream &operator<<(std::ostream &, SiteItem const &);

#endif
