#include <QDebug>
#include "confValue.h"
#include "FunctionItem.h"
#include "GraphModel.h"
#include "RamenValue.h"

#include "chart/TimeChartAutomatonCustomize.h"

TimeChartAutomatonCustomize::TimeChartAutomatonCustomize(
  std::string const &site_,
  std::string const &program_,
  std::string const &function_,
  QObject *parent)
  : conf::Automaton("Customize function", NumStates, parent),
    site(site_),
    program(program_),
    function(function_)
{
  customProgram =
    "tmp/" + srcPathFromProgramName(program) + "/" +
    std::to_string(int64_t(getTime())) + "_" + std::to_string(rand());

  customFunction = "custom_" + function;

  std::string const destKeyPrefix("sources/" + customProgram);

  sourceKey = destKeyPrefix + "/ramen";
  infoKey = destKeyPrefix + "/info";
  /* FIXME: do not wait for the worker but for the addition in the global
   * graphModel, so that the function editors can find it as well! */
  workerKey = "sites/" + site + "/workers/"
            + customProgram + "/"
            + customFunction + "/worker";

  addTransition(WaitSource, WaitInfo, OnSet, sourceKey);
  addTransition(WaitInfo, WaitLockRC, OnSet, infoKey);
  addTransition(WaitLockRC, WaitWorkerOrGraph, OnLock, "target_config");
  addTransition(WaitWorkerOrGraph, WaitGraph, OnSet, workerKey);
  addTransition(WaitWorkerOrGraph, WaitWorker);
  addTransition(WaitWorker, Done, OnSet, workerKey);

  /* Transitions from WaitWorkerOrGraph to WaitWorker and from
   * WaitGraph to Done are manual, triggered by this signal: */
  connect(GraphModel::globalGraphModel, &GraphModel::functionAdded,
          this, &TimeChartAutomatonCustomize::graphChanged);

  start();
}

void TimeChartAutomatonCustomize::graphChanged(
  FunctionItem const *functionItem)
{
  std::shared_ptr<Function const> function(
    std::static_pointer_cast<Function const>(functionItem->shared));
  if (function->name.toStdString() != customFunction ||
      function->programName.toStdString() != customProgram ||
      function->siteName.toStdString() != site)
    return;

  if (currentState == WaitWorkerOrGraph)
    moveTo(WaitWorker);
  else if (currentState == WaitGraph)
    moveTo(Done);
  else
    qCritical() << "TimeChartAutomatonCustomize::graphChanged signaled while"
                   "currently in state" << currentState;
}
