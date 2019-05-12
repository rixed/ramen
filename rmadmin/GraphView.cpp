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

GraphView::GraphView(GraphViewSettings const *settings_, QWidget *parent) :
  QGraphicsView(parent),
  model(nullptr),
  selected(nullptr),
  layoutTimer(this),
  settings(settings_)
{
  QString st = styleSheet();
  std::cout << "ST=" << st.toStdString() << '\n';

  setScene(&scene);

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

void GraphView::select(QModelIndex const &index)
{
  OperationsItem *item =
    static_cast<OperationsItem *>(index.internalPointer());
  if (selected == item) return;
  if (selected) {
    selected->isSelected = false;
    selected->setBorder(2);
  }
  selected = item;
  item->isSelected = true;
  item->setBorder(14);
  item->ensureVisible();

  QPropertyAnimation *borderAnim = new QPropertyAnimation(item, "border");
  borderAnim->setDuration(200);
  borderAnim->setEndValue(4);
  borderAnim->start(QAbstractAnimation::DeleteWhenStopped);
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
  // Redo all arrows every time:
  // TODO: if this stick then simplify the following!
  for (auto it = arrows.begin(); it != arrows.end(); ) {
    GraphArrow *arrow = it->second.first;
    scene.removeItem(arrow);
    delete arrow;  // should remove it from the scene etc...
    it = arrows.erase(it);
  }

  // First, untag all arrows:
  for (auto it = arrows.begin(); it != arrows.end(); it++) {
    it->second.second = false;
  }

# define NB_HMARGINS 3
  int const hmargins[NB_HMARGINS] = {
    settings->siteMarginHoriz + settings->programMarginHoriz + settings->functionMarginHoriz,
    settings->siteMarginHoriz + settings->programMarginHoriz,
    settings->siteMarginHoriz
  };

  for (auto it : relations) {
    FunctionItem const *srcFunction =
      static_cast<FunctionItem const *>(it.first);
    OperationsItem const *src =
      static_cast<OperationsItem const *>(srcFunction);
    OperationsItem const *dst =
      static_cast<OperationsItem const *>(it.second);
    unsigned marginSrc = 0, marginDst = 0;
    unsigned const channel = srcFunction->channel;

    while (src && !src->isVisibleTo(nullptr)) {
      src = src->treeParent;
      marginSrc ++;
    }
    if (! src) continue;  // for some reason even the site is not visible?!
    assert (marginSrc < NB_HMARGINS);

    while (dst && !dst->isVisibleTo(nullptr)) {
      dst = dst->treeParent;
      marginDst ++;
    }
    if (! dst) continue;
    assert (marginDst < NB_HMARGINS);

    // This may happen because of collapsing
    if (src == dst) continue;

    // Do we have this arrow already?
    auto ait = arrows.find(std::pair<OperationsItem const *, OperationsItem const *>(src, dst));
    if (ait == arrows.end()) {
      /*std::cout << "Creating Arrow from " << src->x1 << ", " << src->y1
                << " to " << dst->x0 << ", " << dst->y0 << '\n';*/
      GraphArrow *arrow =
        new GraphArrow(settings,
          src->x1, src->y1, hmargins[marginSrc],
          dst->x0, dst->y0, hmargins[marginDst],
          channel, src->color());
      arrows.insert({{ src, dst }, { arrow, true }});
      arrow->setZValue(-1);
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
  int const max_x = 1 + numNodes/3;
  int const max_y = 1 + numNodes/4;
  layout::solve(&nodes, max_x, max_y);

  QParallelAnimationGroup *animGroup = new QParallelAnimationGroup;
  int const animDuration = 700; // ms

  // Sites must first be positioned, before programs can be positioned
  // in the sites, before functions can be positioned in the programs:
  int const umax = std::numeric_limits<int>::max();

  for (auto siteItem : model->sites) {
    siteItem->x0 = siteItem->y0 = umax;
    siteItem->x1 = siteItem->y1 = 0;

    for (auto programItem : siteItem->programs) {
      for (auto functionItem : programItem->functions) {
        layout::Node &n = nodes[functionIdxs[functionItem]];
        siteItem->x0 = std::min(siteItem->x0, n.x);
        siteItem->y0 = std::min(siteItem->y0, n.y);
        siteItem->x1 = std::max(siteItem->x1, n.x);
        siteItem->y1 = std::max(siteItem->y1, n.y);
      }
    }

    QPointF sitePos =
      settings->pointOfTile(siteItem->x0, siteItem->y0) +
      QPointF(settings->siteMarginHoriz, settings->siteMarginTop);
    QPropertyAnimation *siteAnim = new QPropertyAnimation(siteItem, "pos");
    siteAnim->setDuration(animDuration);
    siteAnim->setEndValue(sitePos);
    animGroup->addAnimation(siteAnim);

    // Now position the programs:
    for (auto programItem : siteItem->programs) {
      programItem->x0 = programItem->y0 = umax;
      programItem->x1 = programItem->y1 = 0;

      for (auto functionItem : programItem->functions) {
        layout::Node &n = nodes[functionIdxs[functionItem]];
        programItem->x0 = std::min(programItem->x0, n.x);
        programItem->y0 = std::min(programItem->y0, n.y);
        programItem->x1 = std::max(programItem->x1, n.x);
        programItem->y1 = std::max(programItem->y1, n.y);
      }
      QPointF progPos =
        settings->pointOfTile(
          programItem->x0 - siteItem->x0,
          programItem->y0 - siteItem->y0) +
        QPointF(settings->programMarginHoriz, settings->programMarginTop);
      QPropertyAnimation *progAnim =
        new QPropertyAnimation(programItem, "pos");
      progAnim->setDuration(animDuration);
      progAnim->setEndValue(progPos);
      animGroup->addAnimation(progAnim);

      // Finally, we can now position the functions:
      for (auto functionItem : programItem->functions) {
        layout::Node &n = nodes[functionIdxs[functionItem]];
        functionItem->x0 = functionItem->x1 = n.x;
        functionItem->y0 = functionItem->y1 = n.y;
        QPointF funcPos =
          settings->pointOfTile(
            n.x - programItem->x0,
            n.y - programItem->y0) +
          QPointF(settings->functionMarginHoriz,
                  settings->functionMarginTop);
        QPropertyAnimation *funcAnim =
          new QPropertyAnimation(functionItem, "pos");
        funcAnim->setDuration(animDuration);
        funcAnim->setEndValue(funcPos);
        animGroup->addAnimation(funcAnim);
      }
    }
  }

  animGroup->start(QAbstractAnimation::DeleteWhenStopped);
  updateArrows(); // or rather when the animation ends?
}
