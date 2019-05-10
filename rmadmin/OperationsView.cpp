#include <QTreeView>
#include "OperationsView.h"
#include "OperationsModel.h"
#include "GraphView.h"

OperationsView::OperationsView(QWidget *parent) :
  QSplitter(parent)
{
  // An OperationsModel satisfies both the TreeView and the GraphView
  // requirements:
  settings = new GraphViewSettings();
  model = new OperationsModel(settings);

  QTreeView *treeView = new QTreeView();
  treeView->setModel(model);
  treeView->setHeaderHidden(true);
  treeView->setUniformRowHeights(true);
  addWidget(treeView);
  QSizePolicy sp = treeView->sizePolicy();
  sp.setHorizontalPolicy(QSizePolicy::Preferred);
  treeView->setSizePolicy(sp);

  GraphView *graphView = new GraphView(settings);
  graphView->setModel(model);
  addWidget(graphView);

  QObject::connect(treeView, &QTreeView::collapsed, graphView, &GraphView::collapse);
  QObject::connect(treeView, &QTreeView::expanded, graphView, &GraphView::expand);
}

OperationsView::~OperationsView()
{
  delete model;
  delete settings;
}
