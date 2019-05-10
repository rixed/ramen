#include <cassert>
#include <cstdlib>
#include <QPropertyAnimation>
#include <QParallelAnimationGroup>
#include "GraphArrow.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "SiteItem.h"
#include "GraphView.h"

GraphView::GraphView(QWidget *parent) :
  QGraphicsView(parent), model(nullptr), layoutTimer(this)
{
  setScene(&scene);
  QSizePolicy sp = sizePolicy();
  sp.setHorizontalPolicy(QSizePolicy::Expanding);
  setSizePolicy(sp);

  layoutTimer.setSingleShot(true);
  QObject::connect(&layoutTimer, &QTimer::timeout, this, &GraphView::startLayout);
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

  // Also the signals allowing us to learn about functions relationships:
  QObject::connect(model, &OperationsModel::relationAdded, this, &GraphView::relationAdded);
  QObject::connect(model, &OperationsModel::relationRemoved, this, &GraphView::relationRemoved);
}

void GraphView::collapse(QModelIndex const &index)
{
  OperationsItem *item =
    static_cast<OperationsItem *>(index.internalPointer());

  item->setCollapsed(true);
  updateArrows();
}

void GraphView::expand(QModelIndex const &index)
{
  OperationsItem *item =
    static_cast<OperationsItem *>(index.internalPointer());

  item->setCollapsed(false);
  updateArrows();
}

void GraphView::insertRows(const QModelIndex &parent, int first, int last)
{
  // Start (or restart) the layoutTimer to trigger a re-layout in 100ms:
  layoutTimer.start(100);

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

void GraphView::updateArrows()
{
  // First, untag all arrows:
  for (auto it = arrows.begin(); it != arrows.end(); it++) {
    it->second.second = false;
  }

  for (auto it : relations) {
    OperationsItem const *src = static_cast<FunctionItem const *>(it.first);
    OperationsItem const *dst = static_cast<FunctionItem const *>(it.second);

    while (src && !src->isVisibleTo(nullptr)) {
      src = src->treeParent;
    }
    if (! src) continue;  // for some reason even the site is not visible?!

    while (dst && !dst->isVisibleTo(nullptr)) {
      dst = dst->treeParent;
    }
    if (! dst) continue;

    // This may happen because of collapsing
    if (src == dst) continue;

    // Do we have this arrow already?
    auto ait = arrows.find(std::pair<OperationsItem const *, OperationsItem const *>(src, dst));
    if (ait == arrows.end()) {
      /*std::cout << "Creating Arrow from " << src->fqName().toStdString()
                << " to " << dst->fqName().toStdString() << std::endl;*/
      GraphArrow *arrow = new GraphArrow(src->anchorOut, dst->anchorIn);
      arrows.insert({{ src, dst }, { arrow, true }});
      scene.addItem(arrow);
    } else {
      ait->second.second = true;
    }
  }

  // Remove all untagged arrows:
  for (auto it = arrows.begin(); it != arrows.end(); ) {
    if (it->second.second) {
      it++;
    } else {
      /*std::cout << "Deleting Arrow from " << it->first.first->fqName().toStdString()
                << " to " << it->first.second->fqName().toStdString() << std::endl;*/
      GraphArrow *arrow = it->second.first;
      scene.removeItem(arrow);
      delete arrow;  // should remove it from the scene etc...
      it = arrows.erase(it);
    }
  }

  /* For some reason Qt is not smart enough to figure out what part of the
   * scene to redraw: */
  scene.update();
}

void GraphView::relationAdded(FunctionItem const *parent, FunctionItem const *child)
{
  //std::cout << "Add " << parent->fqName().toStdString() << "->" << child->fqName().toStdString() << std::endl;
  relations.insert(std::pair<FunctionItem const *, FunctionItem const *>(parent, child));
  updateArrows();
}

void GraphView::relationRemoved(FunctionItem const *parent, FunctionItem const *child)
{
  std::cout << "Del " << parent->fqName().toStdString() << "->" << child->fqName().toStdString() << std::endl;
  auto it = relations.find(parent);
  if (it != relations.end()) {
    relations.erase(it);
    updateArrows();
  } else {
    std::cerr << "Removal of an unknown relation (good riddance!)" << std::endl;
  }
}

void GraphView::startLayout()
{
  std::cout << "Starting a re-layout of the functions" << std::endl;

  if (! model) {
    std::cerr << "Cannot relayout without a model\n";
    return;
  }

  QParallelAnimationGroup *animGroup = new QParallelAnimationGroup;
  int const animDuration = 700; // ms

  // Test: just move the functions around:
  for (auto siteItem : model->sites) {
    QPropertyAnimation *animSite = new QPropertyAnimation(siteItem, "pos");
    animSite->setDuration(animDuration);
    animSite->setEndValue(QPoint(rand() % 500 - 250, rand() % 500 - 250));
    animGroup->addAnimation(animSite);

    for (auto programItem : siteItem->programs) {
      QPropertyAnimation *animProgram = new QPropertyAnimation(programItem, "pos");
      animProgram->setDuration(animDuration);
      animProgram->setEndValue(QPoint(rand() % 200 - 100, rand() % 200 - 100));
      animGroup->addAnimation(animProgram);

      for (auto functionItem : programItem->functions) {
        QPropertyAnimation *animFunction = new QPropertyAnimation(functionItem, "pos");
        animFunction->setDuration(animDuration);
        animFunction->setEndValue(QPoint(rand() % 30 - 15, rand() % 30 - 15));
        animGroup->addAnimation(animFunction);
      }
    }
  }

  animGroup->start(QAbstractAnimation::DeleteWhenStopped);
}
