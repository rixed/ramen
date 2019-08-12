#ifndef SITEITEM_H_190509
#define SITEITEM_H_190509
#include <optional>
#include <vector>
#include "GraphItem.h"

class GraphViewSettings;
class ProgramItem;

struct Site : public GraphData
{
  std::optional<bool> isMaster;

  Site(QString const &name_) : GraphData(name_) {}
};

class SiteItem : public GraphItem
{
protected:
  std::vector<std::pair<QString const, QString const>> labels() const;

public:
  std::vector<ProgramItem *> programs;

  SiteItem(GraphItem *treeParent, std::unique_ptr<Site>, GraphViewSettings const *);

  void reorder(GraphModel const *);
  QRectF operationRect() const;
  bool isTopHalf() const { return false; }
};

std::ostream &operator<<(std::ostream &, SiteItem const &);

#endif
