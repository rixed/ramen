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
#include "ProgramInfoBox.h"
#include "CodeEdit.h"
#include "Chart.h"
#include "ChartDataSet.h"
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
  // And the other way arround:
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
  connect(this, &OperationsView::programSelected, this, &OperationsView::addProgInfo);
  connect(this, &OperationsView::programSelected, this, &OperationsView::addSource);
  connect(this, &OperationsView::functionSelected, this, &OperationsView::addFuncInfo);
  connect(this, &OperationsView::functionSelected, this, &OperationsView::addTail);
  connect(dataTabs, &QTabWidget::tabCloseRequested, this, &OperationsView::remTail);
}

OperationsView::~OperationsView()
{
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

static bool tryFocus(QTabWidget *w, QString const &label)
{
  for (int i = 0; i < w->count(); i++) {
    if (w->tabText(i) == label) {
      w->setCurrentIndex(i);
      return true;
    }
  }
  return false;
}

static void focusLast(QTabWidget *w)
{
  w->setCurrentIndex(w->count() - 1);
}

void OperationsView::addProgInfo(ProgramItem const *p)
{
  if (tryFocus(infoTabs, p->name)) return;

  ProgramInfoBox *box = new ProgramInfoBox(p);
  infoTabs->addTab(box, p->name);
  focusLast(infoTabs);
}

void OperationsView::addSource(ProgramItem const *p)
{
  QString src_file("TODO:src_file of " + p->name);
  if (tryFocus(dataTabs, src_file)) return;

  // TODO: find the src_file in the kvs tree, or do nothing.
  CodeEdit *editor = new CodeEdit(p);
  dataTabs->addTab(editor, src_file);
  focusLast(dataTabs);
}

void OperationsView::addFuncInfo(FunctionItem const *f)
{
  QString label(f->fqName());
  if (tryFocus(infoTabs, label)) return;

  FunctionInfoBox *box = new FunctionInfoBox(f);
  infoTabs->addTab(box, label);
  focusLast(infoTabs);
}

void OperationsView::addTail(FunctionItem *f)
{
  QString label(f->fqName());
  if (tryFocus(dataTabs, label)) return;

  /* TODO: When we select one or more column headers, enable two buttons:
   * one to open a quick chart window with that/those field(s).
   * The other is to add those fields into a named chart.
   * Named charts are saved on the server side, can be added to dashboards,
   * etc.
   * Once in a chart, timeseries can serve various purpose depending on the
   * type of the chart. The possible types depend on the number of time series
   * and their type. The type of a chart, how timeseries are used, the colors,
   * title, etc, are attributes that can be changed at any time; no need to
   * build a new chart. But if we wanted to have several different views of
   * the same data we cold clone a chart.
   * But for now, all we want is the quick-view window, that's added to the
   * data tabs and given a temporary name (usable to add new fields into it
   * as with any other chart. Unless a chart is "saved" it will be definitively
   * forgotten once its tab is closed. */
  if (! f->tailModel) {
    f->tailModel = new TailModel(f);
  }
  f->tailModel->setUsed(true);

  TailTable *table = new TailTable(f->tailModel);
  dataTabs->addTab(table, label);
  focusLast(dataTabs);

  // Add a Plot when the user ask for it:
  connect(table, &TailTable::quickPlotClicked, this, [this, f](QList<int> const &selectedColumns) {
    this->addQuickPlot(f, selectedColumns);
  });
}

void OperationsView::addQuickPlot(FunctionItem const *f, QList<int> const &selectedColumns)
{
  TailTable *table = dynamic_cast<TailTable *>(sender());
  if (! table) {
    std::cout << "Received addQuickPlot from non TailTable!?" << std::endl;
    return;
  }

  // TODO: uniquify the name:
  QString name("Plot: " + f->fqName());

  Chart *chart = new Chart;

  std::shared_ptr<conf::RamenType const> outType = f->outType();
  /* Make a chartDataSet out of each column: */
  for (auto col : selectedColumns) {
    ChartDataSet *ds = new ChartDataSet(f, col);
    chart->addData(ds);
  }
  dataTabs->addTab(chart, name);
  focusLast(dataTabs);
  chart->update();
}

void OperationsView::remTail(int index)
{
  QWidget *w = dataTabs->widget(index);
  if (!w) return;
  TailTable *t = dynamic_cast<TailTable *>(w);
  if (!t) return;
  QAbstractItemModel *m = t->model();
  if (!m) return;
  TailModel *tailModel = dynamic_cast<TailModel *>(m);
  if (!tailModel) return;
  tailModel->setUsed(false);  // schedule for destruction after a while
}

void OperationsView::closeInfo(int idx)
{
  infoTabs->removeTab(idx);
}

void OperationsView::closeData(int idx)
{
  dataTabs->removeTab(idx);
}
