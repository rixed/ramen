#include <QTreeView>
#include "OperationsView.h"
#include "OperationsModel.h"
#include "GraphView.h"

OperationsView::OperationsView(QWidget *parent) :
  QSplitter(parent)
{
  // An OperationsModel satisfies both the TreeView and the GraphView
  // requirements:
  model = new OperationsModel();

  QTreeView *treeView = new QTreeView(this);
  treeView->setModel(model);
  addWidget(treeView);

  GraphView *graphView = new GraphView(this);
  graphView->setModel(model);
  addWidget(graphView);
}

OperationsView::~OperationsView()
{
  delete model;
}
