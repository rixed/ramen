#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QItemSelectionModel>
#include "OperationsView.h"
#include "GraphModel.h"
#include "GraphView.h"

/* For some unfathomable reason the QTreeView sizeHint always return a width
 * of 256, and this is read only. So to change the actual default size of a
 * QTreeView, it seems the only way is to subclass it: */
class NarrowTreeView : public QTreeView
{
public:
  NarrowTreeView(QWidget *parent = NULL) : QTreeView(parent) {}
  QSize sizeHint() const { return QSize(100, 42); }
};

OperationsView::OperationsView(QWidget *parent) :
  QSplitter(parent)
{
  // A GraphModel satisfies both the TreeView and the GraphView
  // requirements:
  settings = new GraphViewSettings();
  graphModel = new GraphModel(settings);

  QWidget *leftPannel = new QWidget(this);
  QVBoxLayout *leftPannelLayout = new QVBoxLayout;
  leftPannelLayout->setContentsMargins(1, 1, 1, 1);
  leftPannelLayout->setSpacing(3);

  QWidget *LODBar = new QWidget;
  QHBoxLayout *LODBarLayout = new QHBoxLayout;
  LODBarLayout->setContentsMargins(1, 1, 1, 1);
  LODBarLayout->setSpacing(3);
  toSites = new QRadioButton("&sites", LODBar);
  toPrograms = new QRadioButton("&programs", LODBar);
  toFunctions = new QRadioButton("&functions", LODBar);
  LODBarLayout->addWidget(toSites);
  LODBarLayout->addWidget(toPrograms);
  LODBarLayout->addWidget(toFunctions);
  LODBar->setLayout(LODBarLayout);
  leftPannelLayout->addWidget(LODBar);

  treeView = new NarrowTreeView();
  treeView->setModel(graphModel);
  treeView->setHeaderHidden(true);
  treeView->setUniformRowHeights(true);
  QSizePolicy sp = treeView->sizePolicy();
  sp.setHorizontalPolicy(QSizePolicy::Preferred);
  sp.setHorizontalStretch(1);
  treeView->setSizePolicy(sp);
  leftPannelLayout->addWidget(treeView);

  leftPannel->setLayout(leftPannelLayout);

  addWidget(leftPannel);

  GraphView *graphView = new GraphView(settings);
  sp = graphView->sizePolicy();
  sp.setHorizontalPolicy(QSizePolicy::Ignored);
  sp.setHorizontalStretch(2);
  graphView->setSizePolicy(sp);
  graphView->setModel(graphModel);
  addWidget(graphView);

  setSizePolicy(QSizePolicy(QSizePolicy::Ignored, QSizePolicy::Ignored));

  // Control the GraphView from the TreeView:
  QObject::connect(treeView, &NarrowTreeView::collapsed, graphView, &GraphView::collapse);
  QObject::connect(treeView, &NarrowTreeView::expanded, graphView, &GraphView::expand);
  QObject::connect(treeView, &NarrowTreeView::clicked, graphView, &GraphView::select);
  // And the other way arround:
  QObject::connect(graphView, &GraphView::selected, treeView, &NarrowTreeView::setCurrentIndex);

  allowReset = true;
  // Control the TreeView from the LOD buttons:
  QObject::connect(toSites, &QRadioButton::clicked, this, &OperationsView::setLOD);
  QObject::connect(toPrograms, &QRadioButton::clicked, this, &OperationsView::setLOD);
  QObject::connect(toFunctions, &QRadioButton::clicked, this, &OperationsView::setLOD);

  // Reset the LOD buttons when manually changing the TreeView:
  QObject::connect(treeView, &NarrowTreeView::collapsed, this, &OperationsView::resetLOD);
  QObject::connect(treeView, &NarrowTreeView::expanded, this, &OperationsView::resetLOD);
}

OperationsView::~OperationsView()
{
  delete graphModel;
  delete settings;
}

// slot to reset the LOD radio buttons
void OperationsView::resetLOD()
{
  if (! allowReset) return;

  toSites->setAutoExclusive(false);
  toPrograms->setAutoExclusive(false);
  toFunctions->setAutoExclusive(false);
  toSites->setChecked(false);
  toPrograms->setChecked(false);
  toFunctions->setChecked(false);
  toSites->setAutoExclusive(true);
  toPrograms->setAutoExclusive(true);
  toFunctions->setAutoExclusive(true);
}

void OperationsView::setLOD(bool)
{
  // collapseAll and friends will unfortunately emit the collapsed signal,
  // which in turn will reset the Radio. So disable this temporarily:
  allowReset = false;
  if (toSites->isChecked()) {
    treeView->collapseAll();
  } else if (toPrograms->isChecked()) {
    treeView->expandToDepth(0);
  } else if (toFunctions->isChecked()) {
    treeView->expandToDepth(1);
  }
  allowReset = true;
}
