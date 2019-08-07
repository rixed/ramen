#ifndef GRAPHMODEL_H_190507
#define GRAPHMODEL_H_190507
#include <vector>
#include <QVector>
#include <QPointF>
#include <QAbstractItemModel>
#include "confValue.h"
#include "confKey.h"
#include "KValue.h"
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

class SiteItem;
class ProgramItem;
class FunctionItem;
class GraphViewSettings;

class GraphModel : public QAbstractItemModel
{
  Q_OBJECT

  // So they can createIndex():
  friend class SiteItem;
  friend class ProgramItem;
  friend class FunctionItem;

  unsigned paletteSize;

  void reorder();
  FunctionItem const *find(QString const &site, QString const &program, QString const &function);

  /* Parents are (re)set when we receive the "worker" object. But some parents
   * may still be unknown, so we also have a pending list of parents, that
   * must also be cleared for that child when its "worker" is received. */
  void addFunctionParent(FunctionItem const *parent, FunctionItem *child);
  void delayAddFunctionParent(FunctionItem *child, QString const &site, QString const &program, QString const &function);
  void removeParents(FunctionItem *child);  // also from pendings!
  void retryAddParents();

  void setFunctionProperty(SiteItem const *, ProgramItem const *, FunctionItem *, QString const &p, std::shared_ptr<conf::Value const>);
  void setProgramProperty(ProgramItem *, QString const &p, std::shared_ptr<conf::Value const>);
  void setSiteProperty(SiteItem *, QString const &p, std::shared_ptr<conf::Value const>);

public:
  GraphViewSettings const *settings;
  std::vector<SiteItem *> sites;

  enum Columns {
    Name = 0,
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
    StatsTotWaitIn,
    StatsTotInputBytes,
    StatsFirstInput,
    StatsLastInput,
    // Stats about internal state:
    StatsNumGroups,
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
    WorkerSrcPath,
    WorkerParams,
    NumParents,
    NumChildren,
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
  static bool columnIsImportant(Columns);

  /* "When subclassing QAbstractItemModel, at the very least you must implement
   * index(), parent(), rowCount(), columnCount(), and data(). These functions
   * are used in all read-only models, and form the basis of editable models."
   */
  QModelIndex index(int , int , QModelIndex const &) const;
  QModelIndex parent(QModelIndex const &) const;
  int rowCount(QModelIndex const &) const;
  int columnCount(QModelIndex const &) const;
  QVariant data(QModelIndex const &, int) const;
  QVariant headerData(int, Qt::Orientation, int) const;

private slots:
  void updateKey(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void positionChanged(QModelIndex const &index) const;
  void functionAdded(FunctionItem const *) const;
  void functionRemoved(FunctionItem const *) const;
  void relationAdded(FunctionItem const *parent, FunctionItem const *child) const;
  void relationRemoved(FunctionItem const *parent, FunctionItem const *child) const;
  void storagePropertyChanged(FunctionItem const *) const;
};

#endif
