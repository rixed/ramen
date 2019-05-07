#include "OperationsView.h"
#include "OperationsModel.h"

OperationsView::OperationsView(QWidget *parent) :
  QTreeView(parent)
{
  OperationsModel *model = new OperationsModel();
  setModel(model);
}

OperationsView::~OperationsView() {}
