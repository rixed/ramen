#include "GraphModel.h"
#include "OperationsView.h"
#include "OperationsWin.h"

OperationsWin::OperationsWin(QWidget *parent) :
  SavedWindow("OperationsGraph", tr("Graph of Operations"), true, parent)
{
  OperationsView *operationsView =
    new OperationsView(GraphModel::globalGraphModel);
  setCentralWidget(operationsView);
}
