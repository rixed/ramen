#include <cassert>
#include "GraphView.h"

GraphView::GraphView(QWidget *parent) :
  QGraphicsView(parent), model(nullptr)
{
  setBackgroundBrush(QBrush(Qt::lightGray, Qt::CrossPattern));
  setRenderHint(QPainter::Antialiasing);
  setScene(&scene);
}

GraphView::~GraphView()
{
}

// Populate the scene with the GraphicsItems:
// Note: we have to pass the model in addition to the parent (despite
// parent.model()), because for root the invalid QModelIndex has nullptr as
// model ; bummer!
void GraphView::populate(QModelIndex const &parent)
{
  int numRows = model->rowCount(parent);
  for (int row = 0; row < numRows; row++) {
    QModelIndex index = model->index(row, 0, parent);
    OperationsItem *i =
      static_cast<OperationsItem *>(index.internalPointer());
    scene.addItem(i);
    populate(index);
  }
}

void GraphView::setModel(OperationsModel const *model_)
{
  assert(! model);
  model = model_;
  // Iterate the model and add a GraphItem for every row:
  //populate(QModelIndex());

  // Connect to the model signals to learn about updates (notice the race
  // condition)
  QObject::connect(model, &OperationsModel::rowsAboutToBeInserted, this, &GraphView::prepareInsertRows);
  QObject::connect(model, &OperationsModel::rowsInserted, this, &GraphView::confirmInsertRows);
  // TODO: same goes for removal
}

void GraphView::collapse(QModelIndex const &index)
{
  // TODO: hide the subnodes and sollapse the connections
  (void)index;
}

void GraphView::expand(QModelIndex const &index)
{
  // TODO: reverse the above
  (void)index;
}

void GraphView::update(QModelIndex const &index)
{
  // TODO: redraw
  (void)index;
}

void GraphView::prepareInsertRows(const QModelIndex &, int, int)
{
  // nothing to do
}

void GraphView::confirmInsertRows(const QModelIndex &parent, int first, int last)
{
  // We only need to add to the scene the toplevel sites:
  if (parent.isValid()) return;

  // Add those new items in the scene:
  for (int row = first ; row <= last; row++) {
    QModelIndex index = model->index(row, 0, parent);
    OperationsItem *item =
      static_cast<OperationsItem *>(index.internalPointer());
    scene.addItem(item);
  }
}
