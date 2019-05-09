#ifndef SITEITEM_H_190509
#define SITEITEM_H_190509
#include <optional>
#include <vector>
#include "OperationsItem.h"

class SiteItem : public OperationsItem
{
public:
  QString name;
  std::optional<bool> isMaster;
  std::vector<ProgramItem *> programs;
  SiteItem(OperationsItem *parent, QString name);
  ~SiteItem();
  QVariant data(int) const;
  void setProperty(QString const &, std::shared_ptr<conf::Value const>);
  void reorder(OperationsModel const *);
};

std::ostream &operator<<(std::ostream &, SiteItem const &);

#endif
