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

bool ProcessesWidgetProxy::filterAcceptsFunction(FunctionItem const &function) const
{
  // Filter out unused functions, optionally:
  if (! includeUnused && ! function.isUsed()) {
    if (verbose)
      std::cout << "Filter out lazy function "
                << function.shared->name.toStdString() << std::endl;
    return false;
  }

  // Filter out the top-halves, optionally:
  if (! includeTopHalves && function.isTopHalf()) {
    std::cout << "Filter out top-half function "
              << function.shared->name.toStdString() << std::endl;
    return false;
  }

  // ...and non-working functions
  if (! includeFinished && ! function.isWorking()) {
    std::cout << "Filter out non-working function "
              << function.shared->name.toStdString() << std::endl;
    return false;
  }

  /* Optionally exclude functions with no pid, unless the function is unused
   * or not working, in which case obviously the function cannot have a pid: */
  if (! includeNonRunning && ! function.isRunning() &&
      (! includeUnused || function.isUsed()) &&
      (! includeFinished || function.isWorking())
  ) {
    std::cout << "Filter out non-running function "
              << function.shared->name.toStdString() << std::endl;
    return false;
  }

  return true;
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

    // Filter entire programs if all their functions are filtered:
    if (0 == program->functions.size()) {
      if (verbose)
        std::cout << "Filter empty program "
                  << program->shared->name.toStdString() << std::endl;
      return false;
    }
    bool accepted = false;
    for (FunctionItem const *function : program->functions) {
      if (filterAcceptsFunction(*function)) {
        accepted = true;
        break;
      }
    }
    if (! accepted) {
      if (verbose)
        std::cout << "Filter out entirely program "
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

  if (! filterAcceptsFunction(*function)) return false;

  SiteItem const *site =
    static_cast<SiteItem const *>(parentProgram->treeParent);

  QString const fq(site->shared->name + ":" +
                   parentProgram->shared->name + "/" +
                   function->shared->name);
  return fq.contains(filterRegExp());
}

void ProcessesWidgetProxy::viewFinished(bool checked)
{
  if (includeFinished == checked) return;
  includeFinished = checked;
  invalidateFilter();
}

void ProcessesWidgetProxy::viewUnused(bool checked)
{
  if (includeUnused == checked) return;
  includeUnused = checked;
  invalidateFilter();
}

void ProcessesWidgetProxy::viewDisabled(bool checked)
{
  if (includeDisabled == checked) return;
  includeDisabled = checked;
  invalidateFilter();
}

void ProcessesWidgetProxy::viewNonRunning(bool checked)
{
  if (includeNonRunning == checked) return;
  includeNonRunning = checked;
  invalidateFilter();
}

void ProcessesWidgetProxy::viewTopHalves(bool checked)
{
  if (includeTopHalves == checked) return;
  includeTopHalves = checked;
  invalidateFilter();
}
