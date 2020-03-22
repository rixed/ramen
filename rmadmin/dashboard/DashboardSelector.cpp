#include "ConfSubTree.h"
#include "dashboard/DashboardTreeModel.h"
#include "UserIdentity.h"

#include "dashboard/DashboardSelector.h"

DashboardSelector::DashboardSelector(
  DashboardTreeModel *model, QWidget *parent)
  : TreeComboBox(parent)
{
  setModel(model);
  setAllowNonLeafSelection(false);

  setSizeAdjustPolicy(QComboBox::AdjustToContents);
}

std::string DashboardSelector::getCurrent() const
{
  assert(my_socket);

  QModelIndex const index(TreeComboBox::getCurrent());
  ConfSubTree const *selected(static_cast<ConfSubTree *>(index.internalPointer()));
  QString const dashName(selected->nameFromRoot("/"));

  if (dashName == "scratchpad") {
    return std::string("clients/" + *my_socket + "/scratchpad");
  } else {
    return std::string("dashboards/" + dashName.toStdString());
  }
}
