#ifndef FUNCTIONITEM_H_190509
#define FUNCTIONITEM_H_190509
#include <memory>
#include <optional>
#include <vector>
#include <QObject>
#include <QString>
#include "confValue.h"
#include "GraphItem.h"

class GraphViewSettings;
class TailModel;

class Function : public QObject, public GraphData
{
  Q_OBJECT

public:
  /* In addition to the name we want the fqName to be available
   * when all we have is a shared_ptr<Function>: */
  QString const fqName;
  /* And the srcPath: */
  std::string const srcPath;

  std::shared_ptr<conf::Worker const> worker;
  std::shared_ptr<conf::RuntimeStats const> runtimeStats;
  std::shared_ptr<conf::TimeRange const> archivedTimes;
  std::optional<int64_t> numArcFiles;
  std::optional<int64_t> numArcBytes;
  std::optional<int64_t> allocArcBytes;
  std::optional<int64_t> pid;
  std::optional<double> lastKilled;
  std::optional<double> lastExit;
  std::optional<QString> lastExitStatus;
  std::optional<int64_t> successiveFailures;
  std::optional<double> quarantineUntil;
  /* instanceSignature is the signature used by supervisor to store a worker
   * state. It's taken from the Worker it's trying to run, and should be equal
   * to worker->workerSign, when we have the worker.
   * In case those disagree we reset either the worker or the instance info,
   * whichever is older. (Warning loudly when a new instance is received before
   * the worker, as it's supposed to happen the other way around.) */
  std::optional<QString> instanceSignature;

  /* Created on demand, deleted after a while when the function is the only
   * reference holder and when the worker changes: */
  std::shared_ptr<TailModel> tailModel;

  Function(
    QString const &name_, QString const &fqName_, std::string const &srcPath_) :
    GraphData(name_),
    fqName(fqName_),
    srcPath(srcPath_) {}

  std::shared_ptr<TailModel> getTail();

  // Returns nullptr if the info is not available yet
  CompiledFunctionInfo const *compiledInfo() const;
  // Returns nullptr is the type is still unknown:
  std::shared_ptr<RamenType const> outType() const;

  void resetInstanceData();
  void checkTail();
};

class FunctionItem : public GraphItem
{
  Q_OBJECT

protected:
  std::vector<std::pair<QString const, QString const>> labels() const;

public:
  // FIXME: Function destructor must clean those:
  // Not the parent in the GraphModel but the parents of the operation:
  std::vector<FunctionItem const *> parents;

  unsigned channel; // could also be used to select a color?

  FunctionItem(
    GraphItem *treeParent, std::unique_ptr<Function>, GraphViewSettings const *);

  int columnCount() const;
  QVariant data(int, int) const;
  QRectF operationRect() const;

  bool isTopHalf() const;
  bool isWorking() const; // has a worker
  bool isRunning() const; // has a pid
  bool isUsed() const; // either not lazy, or have no deps (is_used flag)
};

std::ostream &operator<<(std::ostream &, FunctionItem const &);

#endif
