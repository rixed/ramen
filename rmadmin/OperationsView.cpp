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

  QTreeView *treeView = new QTreeView();
  treeView->setModel(model);
  treeView->setHeaderHidden(true);
  treeView->setUniformRowHeights(true);
  addWidget(treeView);
  QSizePolicy sp = treeView->sizePolicy();
  sp.setHorizontalPolicy(QSizePolicy::Preferred);
  treeView->setSizePolicy(sp);

  GraphView *graphView = new GraphView();
  graphView->setModel(model);
  addWidget(graphView);
}

OperationsView::~OperationsView()
{
  delete model;
}
