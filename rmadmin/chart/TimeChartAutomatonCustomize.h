#ifndef TIMECHARTAUTOMATONCUSTOMIZE_H_200325
#define TIMECHARTAUTOMATONCUSTOMIZE_H_200325
/* Specialization of an Automaton for the customize process.
 * Specialization is required because we want to access some state
 * variable (such as key names) from the slots. */
#include <memory>
#include <string>
#include "confAutomaton.h"

class FunctionItem;
namespace conf {
  struct RamenValueValue;
};

class TimeChartAutomatonCustomize : public conf::Automaton {
  Q_OBJECT

public:
  enum CustomizeSteps {
    // Wait for the new source to be created:
    WaitSource,
    // Wait for the source to be compiled:
    WaitInfo,
    // Wait for target_config to be locked:
    WaitLockRC,
    /* Wait either the worker or the graph change.
     * It is not enough to wait only for the worker because to add the function
     * into the editor it must be present in the global GraphModel already.
     * This is a manual step.  We still do this inside the automaton rather
     * than let the caller deal with that last step, in order for this slot
     * lifespan not to exceed that of the automaton. */
    WaitWorkerOrGraph,
    // Wait for the worker once we got the graph change:
    WaitWorker,
    // Wait for the graph change once we had the worker:
    WaitGraph,
    // The way out (because it has no transitions)
    Done,
    NumStates
  };

  std::string const site, program, function;

  std::string customProgram;
  std::string customFunction;
  std::string sourceKey;
  std::string infoKey;
  std::string workerKey;

  TimeChartAutomatonCustomize(
    std::string const &site,
    std::string const &program,
    std::string const &function,
    QObject *parent = nullptr);

private slots:
  void graphChanged(FunctionItem const *functionItem);
};

#endif
