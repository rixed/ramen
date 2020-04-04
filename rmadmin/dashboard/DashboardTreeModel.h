#ifndef DASHBOARDTREEMODEL_H_200320
#define DASHBOARDTREEMODEL_H_200320
#include "conf.h"
#include "ConfTreeModel.h"

class DashboardTreeModel : public ConfTreeModel
{
  Q_OBJECT

  bool addedScratchpad;
  void addScratchpad();

  void updateNames(std::string const &, KValue const &);
  void deleteNames(std::string const &, KValue const &);

public:
  static DashboardTreeModel *globalDashboardTree;

  DashboardTreeModel(QObject *parent = nullptr);

protected slots:
  void onChange(QList<ConfChange> const &);
};

#endif
