#ifndef DASHBOARDTREEMODEL_H_200320
#define DASHBOARDTREEMODEL_H_200320
#include "ConfTreeModel.h"

class DashboardTreeModel : public ConfTreeModel
{
  Q_OBJECT

  bool addedScratchpad;
  void addScratchpad();

public:
  static DashboardTreeModel *globalDashboardTree;

  DashboardTreeModel(QObject *parent = nullptr);

protected slots:
  void updateNames(std::string const &, KValue const &);
  void deleteNames(std::string const &, KValue const &);
};

#endif
