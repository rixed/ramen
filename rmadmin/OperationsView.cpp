#include <QDebug>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QItemSelectionModel>
#include <QRadioButton>
#include <QTreeView>
#include <QGridLayout>
#include "GraphModel.h"
#include "TailModel.h"
#include "TailTable.h"
#include "GraphView.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "FunctionInfoBox.h"
#include "Chart.h"
#include "widgetTools.h"
#include "OperationsView.h"

/* For some unfathomable reason the QTreeView sizeHint always return a width
 * of 256, and this is read only. So to change the actual default size of a
 * QTreeView, it seems the only way is to subclass it: */
class NarrowTreeView : public QTreeView
{
public:
  NarrowTreeView(QWidget *parent = nullptr) : QTreeView(parent) {}
  QSize sizeHint() const { return QSize(100, 42); }
};

OperationsView::OperationsView(GraphModel *graphModel, QWidget *parent) :
  QSplitter(parent)
{
  // Split the window horizontally:
  setOrientation(Qt::Vertical);

  // On the top side, we have another splitter to separate the treeview
  // from the graphview:
  QSplitter *topSplit = new QSplitter(this);

  QWidget *leftPannel = new QWidget;
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
  leftPannelLayout->addWidget(treeView);

  leftPannel->setLayout(leftPannelLayout);

  topSplit->addWidget(leftPannel);
  topSplit->setStretchFactor(0, 0);

  GraphView *graphView = new GraphView(graphModel->settings);
  graphView->setModel(graphModel);
  topSplit->addWidget(graphView);
  topSplit->setStretchFactor(1, 1);

  // Then the bottom part is made of a info box and a tabbed stack of
  // TailTable.
  QSplitter *bottomSplit = new QSplitter(this);
  infoTabs = new QTabWidget(bottomSplit);
  infoTabs->setTabsClosable(true);
  connect(infoTabs, &QTabWidget::tabCloseRequested, this, &OperationsView::closeInfo);
  dataTabs = new QTabWidget(bottomSplit);
  dataTabs->setTabsClosable(true);
  connect(dataTabs, &QTabWidget::tabCloseRequested, this, &OperationsView::closeData);
  bottomSplit->setStretchFactor(0, 0);
  bottomSplit->setStretchFactor(1, 1);

  // Control the GraphView from the TreeView:
  connect(treeView, &NarrowTreeView::collapsed, graphView, &GraphView::collapse);
  connect(treeView, &NarrowTreeView::expanded, graphView, &GraphView::expand);
  connect(treeView, &NarrowTreeView::clicked, graphView, &GraphView::select);
  // And the other way around:
  connect(graphView, &GraphView::selected, treeView, &NarrowTreeView::setCurrentIndex);

  allowReset = true;
  // Control the TreeView from the LOD buttons:
  connect(toSites, &QRadioButton::clicked, this, &OperationsView::setLOD);
  connect(toPrograms, &QRadioButton::clicked, this, &OperationsView::setLOD);
  connect(toFunctions, &QRadioButton::clicked, this, &OperationsView::setLOD);

  // Reset the LOD buttons when manually changing the TreeView:
  connect(treeView, &NarrowTreeView::collapsed, this, &OperationsView::resetLOD);
  connect(treeView, &NarrowTreeView::expanded, this, &OperationsView::resetLOD);

  // Also let the infobox and tail-tabs know when a function is selected:
  connect(graphView, &GraphView::selected, this, &OperationsView::selectItem);
  connect(this, &OperationsView::programSelected, this, &OperationsView::addSource);
  connect(this, &OperationsView::functionSelected, this, &OperationsView::addFuncInfo);
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

void OperationsView::selectItem(QModelIndex const &index)
{
  if (! index.isValid()) return;
  GraphItem *gi =
    static_cast<GraphItem *>(index.internalPointer());
  ProgramItem *p = dynamic_cast<ProgramItem *>(gi);
  if (p) {
    emit programSelected(p);
    return;
  }

  FunctionItem *f = dynamic_cast<FunctionItem *>(gi);
  if (f) {
    emit functionSelected(f);
    return;
  }
}

void OperationsView::addSource(ProgramItem const *)
{
  // TODO: show the program in the SourcesView
  qDebug() << "TODO";
}

void OperationsView::addFuncInfo(FunctionItem const *f)
{
  QString label(f->fqName());
  if (tryFocusTab(infoTabs, label)) return;

  FunctionInfoBox *box = new FunctionInfoBox(f);
  infoTabs->addTab(box, label);
  focusLastTab(infoTabs);
}

void OperationsView::closeInfo(int idx)
{
  infoTabs->removeTab(idx);
}

void OperationsView::closeData(int idx)
{
  dataTabs->removeTab(idx);
}
