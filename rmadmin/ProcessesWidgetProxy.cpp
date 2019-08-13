#include <iostream>
#include <cassert>
#include <QModelIndex>
#include "GraphItem.h"
#include "SiteItem.h"
#include "ProgramItem.h"
#include "FunctionItem.h"
#include "ProcessesWidgetProxy.h"

static bool const verbose = true;

ProcessesWidgetProxy::ProcessesWidgetProxy(QObject *parent) :
  QSortFilterProxyModel(parent)
{
  setDynamicSortFilter(true);
}

bool ProcessesWidgetProxy::filterAcceptsRow(
  int sourceRow, QModelIndex const &sourceParent) const
{
  if (! sourceParent.isValid()) return true;

  /* For now keep it simple: Accept all sites and programs, filter only
   * function names. */
  GraphItem const *parentPtr =
    static_cast<GraphItem const *>(sourceParent.internalPointer());

  SiteItem const *parentSite =
    dynamic_cast<SiteItem const *>(parentPtr);
  if (parentSite) {
    /* If that program is running only top-halves or non-working functions,
     * then also filter it. There is a vicious consequence though: if it's
     * just empty, and we later add a function that should not be filtered,
     * then the filters won't be updated and the program and functions
     * would stay hidden.
     * Note that setRecursiveFilteringEnabled(false) is of no help here,
     * as it seems to operate the other way around (and false is the default
     * value anyway).
     * The only safe way out of this issue is to invalidate the filter each
     * time we add a function (see later when we connect to endInsertrows).
     * Sites causes no such trouble because we always display even empty
     * sites. */
    assert((size_t)sourceRow < parentSite->programs.size());
    if (verbose)
      std::cout << "Filtering program #" << sourceRow << "?" << std::endl;
    ProgramItem const *program = parentSite->programs[sourceRow];
    if (! includeTopHalves && program->isTopHalf()) {
      if (verbose)
        std::cout << "Filter out top-half program "
                  << program->shared->name.toStdString() << std::endl;
      return false;
    }
    if (! includeStopped && ! program->isWorking()) {
      if (verbose)
        std::cout << "Filter out non-working program "
                  << program->shared->name.toStdString() << std::endl;
      return false;
    }
    return true;
  }

  ProgramItem const *parentProgram =
    dynamic_cast<ProgramItem const *>(parentPtr);
  if (! parentProgram) {
    std::cerr << "Filtering the rows of a function?!" << std::endl;
    return false;
  }

  /* When the parent is a program, build the FQ name of the function
   * and match that: */
  assert((size_t)sourceRow < parentProgram->functions.size());
  FunctionItem const *function = parentProgram->functions[sourceRow];

  // Filter out the top-halves, optionally:
  if (! includeTopHalves && function->isTopHalf()) {
    std::cout << "Filter out top-half function "
              << function->shared->name.toStdString()
              << std::endl;
    return false;
  }

  // ...and non-working functions
  if (! includeStopped && ! function->isWorking()) {
    std::cout << "Filter out non-working function "
              << function->shared->name.toStdString()
              << std::endl;
    return false;
  }

  SiteItem const *site =
    static_cast<SiteItem const *>(parentProgram->treeParent);

  QString const fq(site->shared->name + ":" +
                   parentProgram->shared->name + "/" +
                   function->shared->name);
  return fq.contains(filterRegExp());
}

void ProcessesWidgetProxy::viewTopHalves(bool checked)
{
  if (includeTopHalves == checked) return;
  includeTopHalves = checked;
  invalidateFilter();
}

void ProcessesWidgetProxy::viewStopped(bool checked)
{
  if (includeStopped == checked) return;
  includeStopped = checked;
  invalidateFilter();
}
