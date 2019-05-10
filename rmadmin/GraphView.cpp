#include <cassert>
#include <cstdlib>
#include <limits>
#include <QPropertyAnimation>
#include <QParallelAnimationGroup>
#include "GraphArrow.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "SiteItem.h"
#include "layout.h"
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

static QPointF point_of_tile(unsigned x, unsigned y)
{
  return QPointF(x * 250, y * 150);
}

/* Layout is done on the client side for flexibility: so are we free to
 * have a different layout depending on the collapsing state if we decide
 * to take this road, etc.
 * Also, it's easier for the user to interactively update the layout this
 * way.
 * We can still save the result on the server if we want to share the layout
 * with other users. */
void GraphView::startLayout()
{
  std::cout << "Starting a re-layout of the functions" << std::endl;

  if (! model) {
    std::cerr << "Cannot relayout without a model\n";
    return;
  }

  /* Prepare the problem for the solver: */
  std::vector<layout::Node> nodes;
  nodes.reserve(100);
  std::map<FunctionItem const *, size_t> functionIdxs;

  // First pass: the functions:
  for (auto siteItem : model->sites) {
    for (auto programItem : siteItem->programs) {
      for (auto functionItem : programItem->functions) {
        functionIdxs.emplace(functionItem, nodes.size());
        nodes.emplace_back(siteItem->name.toStdString(),
                           programItem->name.toStdString(),
                           functionItem->name.toStdString());
      }
    }
  }
  // Second pass: the parents:
  for (auto siteItem : model->sites) {
    for (auto programItem : siteItem->programs) {
      for (auto functionItem : programItem->functions) {
        for (auto parent : functionItem->parents) {
          nodes[ functionIdxs[functionItem] ].addParent(functionIdxs[parent]);
        }
      }
    }
  }
  size_t const numNodes = nodes.size();
  unsigned const max_x = 1 + numNodes/3;
  unsigned const max_y = 1 + numNodes/4;
  layout::solve(&nodes, max_x, max_y);

  QParallelAnimationGroup *animGroup = new QParallelAnimationGroup;
  int const animDuration = 700; // ms

  // Sites must first be positioned, before programs can be positioned
  // in the sites, before functions can be positioned in the programs:
  unsigned const umax = std::numeric_limits<unsigned>::max();

  for (auto siteItem : model->sites) {
    unsigned site_min_x = umax;
    unsigned site_min_y = umax;

    for (auto programItem : siteItem->programs) {
      for (auto functionItem : programItem->functions) {
        layout::Node &n = nodes[functionIdxs[functionItem]];
        site_min_x = std::min(site_min_x, n.x);
        site_min_y = std::min(site_min_y, n.y);
      }
    }

    if (siteItem->name == "logger2")
      std::cout << "logger2 position = " << site_min_x << ", " << site_min_y << '\n';
    QPointF sitePos = point_of_tile(site_min_x, site_min_y);
    QPropertyAnimation *siteAnim = new QPropertyAnimation(siteItem, "pos");
    siteAnim->setDuration(animDuration);
    siteAnim->setEndValue(sitePos);
    animGroup->addAnimation(siteAnim);

    // Now position the programs:
    for (auto programItem : siteItem->programs) {
      unsigned prog_min_x = umax;
      unsigned prog_min_y = umax;
      for (auto functionItem : programItem->functions) {
        layout::Node &n = nodes[functionIdxs[functionItem]];
        prog_min_x = std::min(prog_min_x, n.x - site_min_x);
        prog_min_y = std::min(prog_min_y, n.y - site_min_y);
      }
      if (siteItem->name == "logger2" && programItem->name == "logs")
        std::cout << "logger2/logs position = " << prog_min_x << ", " << prog_min_y << '\n';
      QPointF progPos = point_of_tile(prog_min_x, prog_min_y);
      QPropertyAnimation *progAnim =
        new QPropertyAnimation(programItem, "pos");
      progAnim->setDuration(animDuration);
      progAnim->setEndValue(progPos);
      animGroup->addAnimation(progAnim);

      // finally, we can now position the functions:
      for (auto functionItem : programItem->functions) {
        layout::Node &n = nodes[functionIdxs[functionItem]];
        unsigned func_min_x = n.x - site_min_x - prog_min_x;
        unsigned func_min_y = n.y - site_min_y - prog_min_y;
        QPointF funcPos = point_of_tile(func_min_x, func_min_y);
        if (siteItem->name == "logger2" && programItem->name == "logs" && functionItem->name == "http")
          std::cout << "logger2/logs/http position = " << func_min_x << ", " << func_min_y << '\n';
        QPropertyAnimation *funcAnim =
          new QPropertyAnimation(functionItem, "pos");
        funcAnim->setDuration(animDuration);
        funcAnim->setEndValue(funcPos);
        animGroup->addAnimation(funcAnim);
      }
    }
  }

  animGroup->start(QAbstractAnimation::DeleteWhenStopped);
}
