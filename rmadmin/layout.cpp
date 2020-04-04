#include <cassert>
#include <chrono>
#include <cmath>
#include <limits>
#include <map>
#include <vector>
#include <QDebug>
#include <GraphItem.h>
#include <SiteItem.h>
#include <ProgramItem.h>
#include <FunctionItem.h>
#include "layout.h"

static bool const verbose(false);

namespace layout {

static unsigned numUnrankedParents(GraphItem const &n)
{
  unsigned c = 0;
  for (GraphItem *par : n.parentOps) {
    if (par->xRank < 0) c++;
  }
  return c;
}

/* A function that takes a subset of the nodes and return their rank.
 * rank is read from and written into the x coordinate. Negative values mean
 * "unknown". */
template<class I>
static void rank(
  std::vector<I *> const &nodes, size_t toRank[], size_t numToRank, int xRank)
{
  if (0 == numToRank) return;

  /* Extract those with the minimum number of unranked parents: */
  unsigned minUnrankedParents = std::numeric_limits<unsigned>::max();
  size_t firstRank[nodes.size()];
  size_t numFirstRank = 0;
  size_t leftToRank[numToRank];
  size_t numLeftToRank = 0;
  for (size_t i = 0; i < numToRank; i++) {
    size_t n = toRank[i];

    unsigned p = numUnrankedParents(*nodes[n]);
    if (p < minUnrankedParents) {
      minUnrankedParents = p;
      // Reject the current first rank:
      for (size_t f = 0; f < numFirstRank; f++) {
        assert(numLeftToRank < numToRank);
        leftToRank[numLeftToRank++] = firstRank[f];
      }
      firstRank[0] = n;
      numFirstRank = 1;
    } else if (p == minUnrankedParents) {
      assert(numFirstRank < nodes.size());
      firstRank[numFirstRank++] = n;
    } else {
      assert(numLeftToRank < numToRank);
      leftToRank[numLeftToRank++] = n;
    }
  }

  /* And rank them:
   * Note: in case there are plenty with same rank, we might want to try
   * some heuristic to pick only a few of them */
  for (size_t f = 0; f < numFirstRank; f++) {
    assert(nodes[firstRank[f]]->xRank < 0);
    nodes[firstRank[f]]->xRank = xRank;
    nodes[firstRank[f]]->yRank = f;
  }

  /* Recurse */
  rank<I>(nodes, leftToRank, numLeftToRank, xRank + 1);
}

/* Assuming all children are already located within par, compute the size of
 * par */
template<class P, class C>
static void setRelPosition(P *par, std::vector<C *> const &children)
{
  par->x0 = par->y0 = par->x1 = par->y1 = 0;
  for (GraphItem *child : children) {
    if (child->x1 > par->x1) par->x1 = child->x1;
    if (child->y1 > par->y1) par->y1 = child->y1;
  }
}

template<class I>
static int maxRankOfVector(std::vector<I *> const nodes)
{
  int maxRank = 0;
  for (GraphItem const *node : nodes) {
    if (node->xRank > maxRank) maxRank = node->xRank;
  }
  return maxRank;
}

/* Given all the items have sized properly but located at (0,0), move them to
 * the right according to their xRank: */
template<class I>
static void spreadByRank(std::vector<I *> const nodes)
{
  unsigned maxRank = maxRankOfVector<I>(nodes);

  int maxWidths[maxRank+1];
  for (unsigned i = 0; i < maxRank+1; i++) maxWidths[i] = 0;
  for (GraphItem const *node : nodes) {
    int const width = 1 + node->x1 - node->x0;
    if (width > maxWidths[node->xRank]) maxWidths[node->xRank] = width;
  }

  int x0[maxRank+1];
  x0[0] = 0;
  for (unsigned i = 1; i < maxRank+1; i++)
    x0[i] = x0[i-1] + maxWidths[i-1];

  for (GraphItem *node : nodes) {
    int const width = 1 + node->x1 - node->x0;
    int const dx = (maxWidths[node->xRank] - width) / 2;
    node->x0 = x0[node->xRank] + dx;
    node->x1 = node->x0 + width - 1;
  }
}

/* Given a set of nodes make sure each rank is vertically centered: */
template<class I>
static void centerVertically(std::vector<I *> const nodes)
{
  unsigned maxRank = maxRankOfVector<I>(nodes);

  int heights[maxRank+1];
  for (unsigned i = 0; i < maxRank+1; i++) heights[i] = 0;
  for (GraphItem *node : nodes) {
    int const height = 1 + node->y1 - node->y0;
    node->y0 = heights[node->xRank];
    node->y1 = node->y0 + height - 1;
    heights[node->xRank] += height;
  }
  int maxHeight = 0;
  for (unsigned i = 0; i < maxRank+1; i++)
    if (heights[i] > maxHeight) maxHeight = heights[i];

  for (GraphItem *node : nodes) {
    int const dy = (maxHeight - heights[node->xRank]) / 2;
    if (dy > 0) {
      node->y0 += dy;
      node->y1 += dy;
    }
  }
}

static void translateItem(GraphItem *child, GraphItem const *parent)
{
  child->x0 += parent->x0;
  child->x1 += parent->x0;
  child->y0 += parent->y0;
  child->y1 += parent->y0;
}

bool solve(std::vector<SiteItem *> const &sites)
{
  auto start = std::chrono::high_resolution_clock::now();

  /* Initialize the parents (TODO: should be done once and for all when
   * functions are added) */
  /* Also initialize xRank, supposed to be unset (<0) in the beginning: */
  for (SiteItem *site : sites) {
    site->xRank = -1;
    for (ProgramItem *prog : site->programs) {
      prog->xRank = -1;
      for (FunctionItem *func : prog->functions) {
        func->xRank = -1;
        for (FunctionItem *parFunc : func->parents) {
          func->parentOps.insert(parFunc);

          ProgramItem *parProg =
            dynamic_cast<ProgramItem *>(parFunc->treeParent);
          assert(parProg);
          if (parProg != prog) prog->parentOps.insert(parProg);

          SiteItem *parSite =
            dynamic_cast<SiteItem *>(parProg->treeParent);
          assert(parSite);
          if (parSite != site) site->parentOps.insert(parSite);
        }
      }
    }
  }

  /* First round: rank the sites */
  size_t toRank[sites.size()];
  for (size_t s = 0; s < sites.size(); s++) toRank[s] = s;
  rank<SiteItem>(sites, toRank, sites.size(), 0);

  /* For each site, rank every programs */
  for (SiteItem *site : sites) {
    size_t toRank[site->programs.size()];
    for (size_t p = 0; p < site->programs.size(); p++) toRank[p] = p;
    rank<ProgramItem>(site->programs, toRank, site->programs.size(), 0);

    /* And then for each program rank the functions: */
    for (ProgramItem *prog : site->programs) {
      size_t toRank[prog->functions.size()];
      for (size_t f = 0; f < prog->functions.size(); f++) toRank[f] = f;
      rank<FunctionItem>(prog->functions, toRank, prog->functions.size(), 0);
    }
  }

  /* Now assign actual coordinates, for now relative to treeParent: */
  for (SiteItem *site : sites) {
    for (ProgramItem *prog : site->programs) {
      for (FunctionItem *func : prog->functions) {
        /* Each function has size 1x1: */
        func->x0 = func->x1 = func->xRank;
        func->y0 = func->y1 = func->yRank;
      }
      centerVertically<FunctionItem>(prog->functions);
      setRelPosition<ProgramItem, FunctionItem>(prog, prog->functions);
    }
    spreadByRank<ProgramItem>(site->programs);
    centerVertically<ProgramItem>(site->programs);
    setRelPosition<SiteItem, ProgramItem>(site, site->programs);
  }
  spreadByRank<SiteItem>(sites);

  centerVertically<SiteItem>(sites);

  /* Finally, make all coordinate absolute: */
  for (SiteItem *site : sites) {
    /* Site coordinates are already absolute */
    for (ProgramItem *prog : site->programs) {
      translateItem(prog, site);
      for (FunctionItem *func : prog->functions) {
        translateItem(func, prog);
      }
    }
  }

  auto stop = std::chrono::high_resolution_clock::now();
  auto duration =
    std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);

  if (verbose)
    qDebug() << "Layout solved in:" << duration.count() << "ms";

  return true;
}

};
