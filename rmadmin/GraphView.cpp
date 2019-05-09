#include <cassert>
#include "GraphView.h"

GraphView::GraphView(QWidget *parent) :
  QGraphicsView(parent), model(nullptr)
{
  setBackgroundBrush(QBrush(Qt::lightGray, Qt::CrossPattern));
  setRenderHint(QPainter::Antialiasing);
  setScene(&scene);
  QSizePolicy sp = sizePolicy();
  sp.setHorizontalPolicy(QSizePolicy::Expanding);
  setSizePolicy(sp);
}

GraphView::~GraphView()
{
}

QSize GraphView::sizeHint() const
{
  // TODO: compute from components (or rather, cache after components are
  // added/modified)
  return QSize(200, 500);
}

void GraphView::setModel(OperationsModel const *model_)
{
  assert(! model);
  model = model_;

  // Connect to the model signals to learn about updates (notice the race
  // condition)
  QObject::connect(model, &OperationsModel::rowsInserted, this, &GraphView::insertRows);
  // TODO: same goes for removal
}

void GraphView::collapse(QModelIndex const &index)
{
  // TODO: hide the subnodes (recursively) and collapse the connections
  OperationsItem *item =
    static_cast<OperationsItem *>(index.internalPointer());

  item->setCollapsed(true);
}

void GraphView::expand(QModelIndex const &index)
{
  // TODO: reverse the above
  OperationsItem *item =
    static_cast<OperationsItem *>(index.internalPointer());

  item->setCollapsed(false);
}

void GraphView::update(QModelIndex const &index)
{
  // TODO: redraw
  (void)index;
}

void GraphView::insertRows(const QModelIndex &parent, int first, int last)
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
