#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QItemSelectionModel>
#include <QRadioButton>
#include <QTreeView>
#include <QTableView>
#include "GraphModel.h"
#include "TailModel.h"
#include "TailSubModel.h"
#include "GraphView.h"
#include "FunctionItem.h"
#include "OperationsView.h"

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
  tailModel = new TailModel();

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
  QSizePolicy sp = treeView->sizePolicy();
  sp.setHorizontalPolicy(QSizePolicy::Preferred);
  sp.setHorizontalStretch(1);
  treeView->setSizePolicy(sp);
  leftPannelLayout->addWidget(treeView);

  leftPannel->setLayout(leftPannelLayout);

  topSplit->addWidget(leftPannel);

  GraphView *graphView = new GraphView(settings);
  sp = graphView->sizePolicy();
  sp.setHorizontalPolicy(QSizePolicy::Ignored);
  sp.setHorizontalStretch(2);
  graphView->setSizePolicy(sp);
  graphView->setModel(graphModel);
  topSplit->addWidget(graphView);

  topSplit->setSizePolicy(QSizePolicy(QSizePolicy::Ignored, QSizePolicy::Ignored));

  // Then the bottom part is for now just a tabbed view of TailTable.
  tailTabs = new QTabWidget(this);

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

  // Connect the grpahModel to the tailModel:
  QObject::connect(graphModel, &GraphModel::functionAdded, tailModel, &TailModel::addFunction);
  // And make a new tabTail when a function is selected in the graphView:
  QObject::connect(graphView, &GraphView::selected, this, &OperationsView::addTail);
}

OperationsView::~OperationsView()
{
  // Delete tailModel first as it references FunctionItems belonging to the
  // graphModel:
  delete tailModel;
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

void OperationsView::addTail(QModelIndex const &index)
{
  if (! index.isValid()) return;
  GraphItem const *gi =
    static_cast<GraphItem const *>(index.internalPointer());
  FunctionItem const *f = dynamic_cast<FunctionItem const *>(gi);
  if (! f) return;  // interested only in functions

  TailSubModel *subModel = new TailSubModel(tailModel, f);
  if (subModel) {
    QTableView *table = new QTableView;
    table->setModel(subModel);
    tailTabs->addTab(table, f->fqName());
  } else {
    std::cerr << "Cannot find submodel for function "
              << f->fqName().toStdString() << std::endl;
  }
}
