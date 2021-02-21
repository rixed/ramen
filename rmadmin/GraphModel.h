#ifndef GRAPHMODEL_H_190507
#define GRAPHMODEL_H_190507
#include <vector>
#include <QAbstractItemModel>
#include <QPointF>
#include "conf.h"
#include "GraphItem.h"

/* The "Graph" described here is the graph of
 *
 *   site/$SITE/workers/$FQ/(worker|stats...)
 *
 * That we save as a 3 layers tree:
 *
 * - site, with a name and a few other properties like number of workers and
 *   an is_master flag;
 * - programs, with again a name etc;
 * - function, with name, stats, worker etc.
 *
 * The "PerInstance" layer is not feature, but all information about (past)
 * running processes are instead stored in the function (it would not be
 * very stimulating to display a graph of instances distinct from the graph of
 * functions, at least as long as we are supposed to have one process per
 * function).
 *
 * This tree is populated using callbacks from the conf synchroniser.
 *
 * The GraphView uses the same data model. So the model must also be able
 * to return another set of parents (graph ancestors). The graphView slots
 * will be connected to the TreeView signals so that collapse/expand/select
 * are propagated. But as the GraphView does just the displaying (not the
 * graph layout) then the data model most hold the positions itself (the
 * positions are part of the data model, can be edited/saved by users...)
 * Meaning we need yet another member: [position] and another signal:
 * [positionChanged].
 */

class FunctionItem;
class GraphViewSettings;
struct KValue;
class ParsedKey;
class ProgramItem;
class SiteItem;

class GraphModel : public QAbstractItemModel
{
  Q_OBJECT

  // So they can createIndex():
  friend class SiteItem;
  friend class ProgramItem;
  friend class FunctionItem;

  void reorder();
  FunctionItem *find(
    QString const &site, QString const &program, QString const &function);

  /* Parents are (re)set when we receive the "worker" object. But some parents
   * may still be unknown, so we also have a pending list of parents, that
   * must also be cleared for that child when its "worker" is received. */
  void addFunctionParent(FunctionItem *parent, FunctionItem *child);
  void delayAddFunctionParent(
    FunctionItem *child, QString const &site, QString const &program,
    QString const &function);
  void removeParents(FunctionItem *child);  // also from pendings!
  void retryAddParents();

  void setFunctionProperty(
    SiteItem const *, ProgramItem const *, FunctionItem *, ParsedKey const &p,
    std::shared_ptr<conf::Value const>);
  void setProgramProperty(
    ProgramItem *, ParsedKey const &p, std::shared_ptr<conf::Value const>);
  void setSiteProperty(
    SiteItem *, ParsedKey const &p, std::shared_ptr<conf::Value const>);
  void delFunctionProperty(FunctionItem *, ParsedKey const &p);
  void delProgramProperty(ProgramItem *, ParsedKey const &p);
  void delSiteProperty(SiteItem *, ParsedKey const &p);

  void updateKey(std::string const &, KValue const &);
  void deleteKey(std::string const &, KValue const &);

public:
  GraphViewSettings const *settings;
  std::vector<SiteItem *> sites;

  enum Columns {
    Name = 0,
    // Buttons
    ActionButton1,
    ActionButton2,
    // Flags:
    WorkerTopHalf,
    WorkerEnabled,
    WorkerDebug,
    WorkerUsed,
    // The statistics time:
    StatsTime,
    // Stats about inputs:
    StatsNumInputs,
    StatsNumSelected,
    StatsNumFiltered,
    StatsTotWaitIn,
    StatsTotInputBytes,
    StatsFirstInput,
    StatsLastInput,
    // Stats about internal state:
    StatsNumGroups,
    StatsMaxGroups,
    // Stats about outputs:
    StatsNumOutputs,
    StatsTotWaitOut,
    StatsFirstOutput,
    StatsLastOutput,
    StatsTotOutputBytes,
    StatsNumFiringNotifs,
    StatsNumExtinguishedNotifs,
    // Stats about archives:
    NumArcFiles,
    NumArcBytes,
    AllocedArcBytes,
    StatsAverageTupleSize,
    StatsNumAverageTupleSizeSamples,
    ArchivedTimes,
    // Stats about event times:
    StatsMinEventTime,
    StatsMaxEventTime,
    // Stats about resource consumption:
    StatsTotCpu,
    StatsCurrentRam,
    StatsMaxRam,
    // Stats on the process:
    StatsFirstStartup,
    StatsLastStartup,
    WorkerReportPeriod,
    WorkerCWD,
    WorkerSrcPath,
    WorkerParams,
    NumParents,
    NumChildren,
    InstancePid,
    InstanceLastKilled,
    InstanceLastExit,
    InstanceLastExitStatus,
    InstanceSuccessiveFailures,
    InstanceQuarantineUntil,
    InstanceSignature, // = WorkerSignature in theory
    // Internal info:
    WorkerSignature,
    WorkerBinSignature,
    NumTailTuples,
    // Not a column but a mark that must come last:
    NumColumns
  };

  static int const SortRole = Qt::UserRole + 0;

  GraphModel(GraphViewSettings const *, QObject *parent = nullptr);

  static QString const columnName(Columns);
  // Those columns that are displayed by default on the processes list:
  static bool columnIsImportant(Columns);
  // Those columns that are displayed in the storage window:
  static bool columnIsAboutArchives(Columns);

  /* "When subclassing QAbstractItemModel, at the very least you must implement
   * index(), parent(), rowCount(), columnCount(), and data(). These functions
   * are used in all read-only models, and form the basis of editable models."
   */
  QModelIndex index(int, int, QModelIndex const &) const;
  QModelIndex parent(QModelIndex const &) const;
  int rowCount(QModelIndex const &) const;
  int columnCount(QModelIndex const &) const;
  QVariant data(QModelIndex const &, int) const;
  QVariant headerData(int, Qt::Orientation, int) const;
  GraphItem const *itemOfIndex(QModelIndex const &) const;

  static GraphModel *globalGraphModel;

private slots:
  void onChange(QList<ConfChange> const &);

signals:
  void positionChanged(QModelIndex const &index) const;
  void functionAdded(FunctionItem const *) const;
  void functionRemoved(FunctionItem const *) const;
  void relationAdded(FunctionItem const *parent, FunctionItem const *child) const;
  void relationRemoved(FunctionItem const *parent, FunctionItem const *child) const;
  void storagePropertyChanged(FunctionItem const *) const;
  /* Special signal each time a worker changes (to help with filtering
   * processes, see ProcessesWidget.cpp */
  void workerChanged(QString const &oldSign, QString const &newSign) const;
};

#endif
