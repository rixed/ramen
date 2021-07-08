#include <cassert>
#include <list>
#include <QDebug>
#include <QRegularExpression>
#include "GraphModel.h"
#include "confValue.h"
#include "conf.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "SiteItem.h"

static bool const verbose(false);

GraphModel *GraphModel::globalGraphModel;

GraphModel::GraphModel(GraphViewSettings const *settings_, QObject *parent) :
  QAbstractItemModel(parent),
  settings(settings_)
{
  connect(kvs, &KVStore::keyChanged,
          this, &GraphModel::onChange);
}

void GraphModel::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
      case KeyChanged:
        updateKey(change.key, change.kv);
        break;
      case KeyDeleted:
        deleteKey(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

QModelIndex GraphModel::index(int row, int column, QModelIndex const &parent) const
{
  if (! parent.isValid()) { // Asking for a site
    if ((size_t)row >= sites.size()) return QModelIndex();
    SiteItem *site = sites[row];
    assert(site->treeParent == nullptr);
    return createIndex(row, column, static_cast<GraphItem *>(site));
  }

  GraphItem *parentPtr = static_cast<GraphItem *>(parent.internalPointer());
  // Maybe a site?
  SiteItem *parentSite = dynamic_cast<SiteItem *>(parentPtr);
  if (parentSite) { // bingo!
    if ((size_t)row >= parentSite->programs.size()) return QModelIndex();
    ProgramItem *program = parentSite->programs[row];
    assert(program->treeParent == parentPtr);
    return createIndex(row, column, static_cast<GraphItem *>(program));
  }

  // Maybe a program?
  ProgramItem *parentProgram = dynamic_cast<ProgramItem *>(parentPtr);
  if (parentProgram) {
    if ((size_t)row >= parentProgram->functions.size()) return QModelIndex();
    FunctionItem *function = parentProgram->functions[row];
    assert(function->treeParent == parentPtr);
    return createIndex(row, column, static_cast<GraphItem *>(function));
  }

  // There is no alternative
  assert(!"Someone should RTFM on indexing");
}

QModelIndex GraphModel::parent(QModelIndex const &index) const
{
  GraphItem *item =
    static_cast<GraphItem *>(index.internalPointer());
  GraphItem *treeParent = item->treeParent;

  if (! treeParent) {
    // We must be a site then:
    assert(nullptr != dynamic_cast<SiteItem *>(item));
    return QModelIndex(); // parent is "root"
  }

  return createIndex(treeParent->row, 0, treeParent);
}

int GraphModel::rowCount(QModelIndex const &parent) const
{
  if (! parent.isValid()) {
    // That must be "root" then:
    return sites.size();
  }

  GraphItem *parentPtr =
    static_cast<GraphItem *>(parent.internalPointer());
  SiteItem *parentSite = dynamic_cast<SiteItem *>(parentPtr);
  if (parentSite) {
    return parentSite->programs.size();
  }

  ProgramItem *parentProgram = dynamic_cast<ProgramItem *>(parentPtr);
  if (parentProgram) {
    return parentProgram->functions.size();
  }

  FunctionItem *parentFunction = dynamic_cast<FunctionItem *>(parentPtr);
  if (parentFunction) {
    return 0;
  }

  assert(!"how is indexing working, again?");
}

int GraphModel::columnCount(QModelIndex const &parent) const
{
  /* Number of columns for the global header. */
  if (! parent.isValid()) return NumColumns;

  GraphItem *item =
    static_cast<GraphItem *>(parent.internalPointer());
  return item->columnCount();
}

QVariant GraphModel::data(QModelIndex const &index, int role) const
{
  if (! index.isValid()) return QVariant();

  GraphItem const *item = itemOfIndex(index);
  return item->data(index.column(), role);
}

GraphItem const *GraphModel::itemOfIndex(QModelIndex const &index) const
{
  if (! index.isValid()) return nullptr;
  return static_cast<GraphItem *>(index.internalPointer());
}

QString const GraphModel::columnName(GraphModel::Columns c)
{
  switch (c) {
    case Name: return tr("Name");
    case ActionButton1: return QString();
    case ActionButton2: return QString();
    case WorkerTopHalf: return tr("Top-half");
    case WorkerEnabled: return tr("Enabled");
    case WorkerDebug: return tr("Debug");
    case WorkerUsed: return tr("Used");
    case StatsTime: return tr("Stats Emission");
    case StatsNumInputs: return tr("Inputs Events");
    case StatsNumSelected: return tr("Selected Events");
    case StatsNumFiltered: return tr("Filtered Events");
    case StatsTotWaitIn: return tr("Waiting for Input");
    case StatsTotInputBytes: return tr("Input Bytes");
    case StatsFirstInput: return tr("First Input Reception");
    case StatsLastInput: return tr("Last Input Reception");
    case StatsNumGroups: return tr("Groups");
    case StatsMaxGroups: return tr("Groups (max)");
    case StatsNumOutputs: return tr("Output Events");
    case StatsTotWaitOut: return tr("Waiting for Output");
    case StatsFirstOutput: return tr("First Output Emitted");
    case StatsLastOutput: return tr("Last Output Emitted");
    case StatsTotOutputBytes: return tr("Output Bytes");
    case StatsNumFiringNotifs: return tr("Fired Notifications");
    case StatsNumExtinguishedNotifs: return tr("Extinguished Notifications");
    case NumArcFiles: return tr("Archive Files");
    case NumArcBytes: return tr("Archive Bytes");
    case AllocedArcBytes: return tr("Allocated Storage");
    case StatsMinEventTime: return tr("Min. Event Time");
    case StatsMaxEventTime: return tr("Max. Event Time");
    case StatsTotCpu: return tr("Total CPU");
    case StatsCurrentRam: return tr("Current RAM");
    case StatsMaxRam: return tr("Max. RAM");
    case StatsFirstStartup: return tr("First Startup");
    case StatsLastStartup: return tr("Last Startup");
    case StatsAverageTupleSize: return tr("Avg Bytes per Archived Event");
    case StatsNumAverageTupleSizeSamples: return tr("Full Event Size Samples");
    case ArchivedTimes: return tr("Archives Duration");
    case WorkerReportPeriod: return tr("Report Period");
    case WorkerCWD: return tr("Working Directory");
    case WorkerSrcPath: return tr("Source");
    case WorkerParams: return tr("Parameters");
    case NumParents: return tr("Parents");
    case NumChildren: return tr("Children");
    case InstancePid: return tr("PID");
    case InstanceLastKilled: return tr("Last Killed");
    case InstanceLastExit: return tr("Last Exited");
    case InstanceLastExitStatus: return tr("Last Exit Status");
    case InstanceSuccessiveFailures: return tr("Successive Failures");
    case InstanceQuarantineUntil: return tr("Quarantined Until");
    case InstanceSignature: return tr("Instance Signature");
    case WorkerSignature: return tr("Worker Signature");
    case WorkerBinSignature: return tr("Binary Signature");
    case NumTailTuples: return tr("Received Tail Events");
    case NumColumns: break;
  }

  assert(!"Invalid column");
}

bool GraphModel::columnIsImportant(Columns c)
{
  switch (c) {
    case Name:
    case StatsNumInputs:
    case StatsLastInput:
    case StatsNumGroups:
    case StatsNumOutputs:
    case StatsLastOutput:
    case StatsNumFiringNotifs:
    case NumArcBytes:
    case StatsMaxEventTime:
      return true;
    default:
      return false;
  }
}

bool GraphModel::columnIsAboutArchives(Columns c)
{
  switch (c) {
    case Name:
    case NumArcFiles:
    case NumArcBytes:
    case AllocedArcBytes:
    case ArchivedTimes:
      return true;
    default:
      return false;
  }
}

QVariant GraphModel::headerData(
  int section, Qt::Orientation orientation, int role) const
{
  if (role != Qt::DisplayRole || orientation != Qt::Horizontal)
    return QVariant();

  return columnName((GraphModel::Columns)section);
}

void GraphModel::reorder()
{
  for (int i = 0; (size_t)i < sites.size(); i ++) {
    if (sites[i]->row != i) {
      sites[i]->row = i;
      sites[i]->setPos(0, i * 130);
      emit positionChanged(createIndex(i, 0, static_cast<GraphItem *>(sites[i])));
    }
  }
}

class ParsedKey {
public:
  bool valid;
  QString site, program, function, property, instanceSignature;
  ParsedKey(std::string const &k)
  {
    static QRegularExpression re(
      "^sites/(?<site>[^/]+)/"
      "("
        "workers/(?<program>.+)/"
        "(?<function>[^/]+)/"
        "(?<function_property>"
          "worker|"
          "stats/runtime|"
          "archives/(times|num_files|current_size|alloc_size)|"
          "instances/(?<signature>[^/]+)/(?<instance_property>[^/]+)"
        ")"
      "|"
        "(?<site_property>is_master)"
      ")$"
      ,
      QRegularExpression::DontCaptureOption
    );
    assert(re.isValid());
    QString subject = QString::fromStdString(k);
    QRegularExpressionMatch match = re.match(subject);
    valid = match.hasMatch();
    if (valid) {
      site = match.captured("site");
      program = match.captured("program");
      function = match.captured("function");
      // Try the deepest first:
      instanceSignature = match.captured("signature");
      if (instanceSignature.isNull()) {
        property = match.captured("function_property");
      } else {
        property = match.captured("instance_property");
      }
      if (property.isNull()) {
        property = match.captured("site_property");
      }
    }
  }
};

FunctionItem *GraphModel::find(QString const &site, QString const &program, QString const &function)
{
  if (verbose)
    qDebug() << "Look for function" << site << "/" << program << "/" << function;
  for (SiteItem *siteItem : sites) {
    if (siteItem->shared->name == site) {
      for (ProgramItem *programItem : siteItem->programs) {
        if (programItem->shared->name == program) {
          for (FunctionItem *functionItem : programItem->functions) {
            if (functionItem->shared->name == function) {
              return functionItem;
            }
          }
          if (verbose)
            qDebug() << "No such function:" << function;
          return nullptr;
        }
      }
      if (verbose)
        qDebug() << "No such program:" << program;
      return nullptr;
    }
  }
  if (verbose)
    qDebug() << "No such site:" << site;
  return nullptr;
}

void GraphModel::addFunctionParent(FunctionItem *parent, FunctionItem *child)
{
  child->parents.push_back(parent);
  emit relationAdded(parent, child);
}

/* In case we receive a child before its parents we have to wait for the
 * parent before setting up the relationship: */
struct PendingAddParent {
  /* FIXME: FunctionItem destructors should look in here and remove pending
   * AddParent for them! */
  FunctionItem *child;
  QString const site, program, function;

  PendingAddParent(FunctionItem *child_, QString const &site_, QString const &program_, QString const function_) :
    child(child_), site(site_), program(program_), function(function_) {}
};
static std::list<PendingAddParent> pendingAddParents;

void GraphModel::removeParents(FunctionItem *child)
{
  for (size_t i = 0; i < child->parents.size(); i ++) {
    emit relationRemoved(child->parents[i], child);
  }
  child->parents.clear();

  // Also go through the pendingAddParents:
  for (auto it = pendingAddParents.begin(); it != pendingAddParents.end(); ) {
    if (it->child == child) {
      it = pendingAddParents.erase(it);
    } else {
      it ++;
    }
  }
}

void GraphModel::delayAddFunctionParent(FunctionItem *child, QString const &site, QString const &program, QString const &function)
{
  if (verbose)
    qDebug() << "Will wait for parent before connecting to it";
  pendingAddParents.emplace_back(child, site, program, function);
}

void GraphModel::retryAddParents()
{
  for (auto it = pendingAddParents.begin(); it != pendingAddParents.end(); ) {
    FunctionItem *parent = find(it->site, it->program, it->function);
    if (parent) {
      if (verbose)
        qDebug() << "Resolved pending parent";
      addFunctionParent(parent, it->child);
      it = pendingAddParents.erase(it);
    } else {
      it ++;
    }
  }
}

void GraphModel::setFunctionProperty(
  SiteItem const *siteItem, ProgramItem const *programItem,
  FunctionItem *functionItem, ParsedKey const &pk,
  std::shared_ptr<conf::Value const> v)
{
  if (verbose)
    qDebug() << "setFunctionProperty for" << pk.property;

  int changed(0);
# define PROPERTY_CHANGED 0x1
# define STORAGE_CHANGED  0x2
# define WORKER_CHANGED   0x4

  std::shared_ptr<Function> function =
    std::static_pointer_cast<Function>(functionItem->shared);

  if (! pk.instanceSignature.isEmpty()) {
    /* Remember that old instances are not removed from the config tree.
     * Also, worker is supposed to arrive before the instances.
     * So we can safely reject any instance that has not the same signature
     * than the worker.
     * Now, what if we have no worker yet? Given the GraphModel starts
     * listening to the key change before the initial sync starts, and the
     * confserver sends the key in chronological order, then if we add an
     * instance before a worker it can safely be ignored. */
    if (! function->worker ||
        pk.instanceSignature != function->worker->workerSign) {
      if (verbose)
        qDebug() << "Ignoring new instance for "
                 << pk.instanceSignature << "because "
                 << (! function->worker ? "no worker yet" :
                       "its signature does not match worker's");
    } else {
      if (function->instanceSignature.has_value() &&
          *function->instanceSignature != pk.instanceSignature) {
        if (verbose)
          qDebug() << "Resetting old instance from "
                   << *function->instanceSignature << "to "
                   << pk.instanceSignature;
        function->resetInstanceData();
        changed |= PROPERTY_CHANGED + STORAGE_CHANGED;
      }
      function->instanceSignature = pk.instanceSignature;
      changed |= PROPERTY_CHANGED;
    }
  }

  QString prevWorkerSign {
    function->worker ? function->worker->workerSign : QString() };

  if (pk.property == "worker") {
    std::shared_ptr<conf::Worker const> cf =
      std::dynamic_pointer_cast<conf::Worker const>(v);
    if (cf) {
      std::shared_ptr<Site> site =
        std::static_pointer_cast<Site>(siteItem->shared);
      std::shared_ptr<Program> program =
        std::static_pointer_cast<Program>(programItem->shared);

      function->worker = cf;
      changed |= WORKER_CHANGED;

      if (verbose)
        qDebug() << "Setting worker to "
                 << function->worker->workerSign;

      for (auto const &ref : cf->parent_refs) {
        /* If the parent is not local then assume the existence of a top-half
         * for this function running on the remote site: */
        QString psite, pprog, pfunc;
        if (ref->site == site->name) {
          psite = ref->site;
          pprog = ref->program;
          pfunc = ref->function;
        } else {
          psite = ref->site;
          pprog = program->name;
          pfunc = function->name;
        }
        /* Try to locate the GraphItem of this parent. If it's not
         * there yet, enqueue this worker somewhere and revisit this
         * once a new function appears. */
        FunctionItem *parent = find(psite, pprog, pfunc);
        if (parent) {
          if (verbose) qDebug() << "Set immediate parent";
          addFunctionParent(parent, functionItem);
        } else {
          if (verbose) qDebug() << "Set delayed parent";
          delayAddFunctionParent(functionItem, psite, pprog, pfunc);
        }
      }
      changed |= STORAGE_CHANGED;
    }
  } else if (pk.property == "stats/runtime") {
    std::shared_ptr<conf::RuntimeStats const> stats =
      std::dynamic_pointer_cast<conf::RuntimeStats const>(v);
    if (stats) {
      function->runtimeStats = stats;
      changed |= PROPERTY_CHANGED;
    }
  } else if (pk.property == "archives/times") {
    std::shared_ptr<conf::TimeRange const> times =
      std::dynamic_pointer_cast<conf::TimeRange const>(v);
    if (times) {
      function->archivedTimes = times;
      changed |= STORAGE_CHANGED;
    }
  } else if (pk.property == "archives/num_files") {
#   define SET_RAMENVALUE(type, var, whatChanged) do { \
      std::shared_ptr<conf::RamenValueValue const> cf = \
        std::dynamic_pointer_cast<conf::RamenValueValue const>(v); \
      if (cf) { \
        std::shared_ptr<type const> v = \
          std::dynamic_pointer_cast<type const>(cf->v); \
        if (v) { \
          function->var = v->v; \
          changed |= whatChanged; \
        } \
      } \
    } while (0)

    SET_RAMENVALUE(VI64, numArcFiles, STORAGE_CHANGED);
  } else if (pk.property == "archives/current_size") {
    SET_RAMENVALUE(VI64, numArcBytes, STORAGE_CHANGED);
  } else if (pk.property == "archives/alloc_size") {
    SET_RAMENVALUE(VI64, allocArcBytes, STORAGE_CHANGED);
  } else if (pk.property == "pid") {
    /* Worker did not really change, but everything that requires the process list
     * to be invalidated must emit that signal: */
    SET_RAMENVALUE(VU32, pid, PROPERTY_CHANGED | WORKER_CHANGED);
  } else if (pk.property == "last_killed") {
    SET_RAMENVALUE(VFloat, lastKilled, PROPERTY_CHANGED | WORKER_CHANGED);
  } else if (pk.property == "last_exit") {
    SET_RAMENVALUE(VFloat, lastExit, PROPERTY_CHANGED | WORKER_CHANGED);
  } else if (pk.property == "last_exit_status") {
    SET_RAMENVALUE(VString, lastExitStatus, PROPERTY_CHANGED | WORKER_CHANGED);
  } else if (pk.property == "successive_failures") {
    SET_RAMENVALUE(VI64, successiveFailures, PROPERTY_CHANGED);
  } else if (pk.property == "quarantine_until") {
    SET_RAMENVALUE(VFloat, quarantineUntil, PROPERTY_CHANGED);
  } else {
    if (verbose)
      qDebug() << "Useless property" << pk.property;
  }

  if (changed & STORAGE_CHANGED) {
    if (verbose) qDebug() << "Emitting storagePropertyChanged";
    emit storagePropertyChanged(functionItem);
  }
  if (changed & PROPERTY_CHANGED) {
    if (verbose) qDebug() << "Emitting dataChanged";
    QModelIndex topLeft(functionItem->index(this, 0));
    QModelIndex bottomRight(functionItem->index(this, GraphModel::NumColumns - 1));
    emit dataChanged(topLeft, bottomRight, { Qt::DisplayRole });
  }
  if (changed & WORKER_CHANGED) {
    function->checkTail();
    emit workerChanged(
      prevWorkerSign,
      function->worker ? function->worker->workerSign : QString());
  }
}

void GraphModel::delFunctionProperty(
  FunctionItem *functionItem, ParsedKey const &pk)
{
  if (verbose)
    qDebug() << "delFunctionProperty for" << pk.property;

  int changed(0);
  QString prevWorkerSign;

  std::shared_ptr<Function> function =
    std::static_pointer_cast<Function>(functionItem->shared);

  if (pk.property == "worker") {
    if (function->worker) {
      /* As we have connected this function to its parents (not treeParents!)
       * when the worker was received, disconnect it now: */
      removeParents(functionItem);
      changed |= STORAGE_CHANGED;
      if (verbose)
        qDebug() << "Resetting worker "
                 << function->worker->workerSign;
      prevWorkerSign = function->worker->workerSign;
      function->worker.reset();
      changed |= WORKER_CHANGED;
    }
  } else if (pk.property == "stats/runtime") {
    if (function->runtimeStats) {
      function->runtimeStats.reset();
      changed |= PROPERTY_CHANGED;
    }
  } else if (pk.property == "archives/times") {
    if (function->archivedTimes) {
      function->archivedTimes.reset();
      changed |= STORAGE_CHANGED;
    }
  } else if (pk.property == "archives/num_files") {
#   define DEL_RAMENVALUE(var, whatChanged) do { \
      if (function->var.has_value()) { \
        function->var.reset(); \
        changed |= whatChanged; \
      } \
    } while (0)

    DEL_RAMENVALUE(numArcFiles, STORAGE_CHANGED);
  } else if (pk.property == "archives/current_size") {
    DEL_RAMENVALUE(numArcBytes, STORAGE_CHANGED);
  } else if (pk.property == "archives/alloc_size") {
    DEL_RAMENVALUE(allocArcBytes, STORAGE_CHANGED);
  } else {
    if (! pk.instanceSignature.isEmpty() &&
        function->instanceSignature.has_value() &&
        pk.instanceSignature == function->instanceSignature)
    {
      if (pk.property == "pid") {
        DEL_RAMENVALUE(pid, PROPERTY_CHANGED);
      } else if (pk.property == "last_killed") {
        DEL_RAMENVALUE(lastKilled, PROPERTY_CHANGED);
      } else if (pk.property == "last_exit") {
        DEL_RAMENVALUE(lastExit, PROPERTY_CHANGED);
      } else if (pk.property == "last_exit_status") {
        DEL_RAMENVALUE(lastExitStatus, PROPERTY_CHANGED);
      } else if (pk.property == "successive_failures") {
        DEL_RAMENVALUE(successiveFailures, PROPERTY_CHANGED);
      } else if (pk.property == "quarantine_until") {
        DEL_RAMENVALUE(quarantineUntil, PROPERTY_CHANGED);
      }
    }
  }

  if (changed & STORAGE_CHANGED) {
    if (verbose) qDebug() << "Emitting storagePropertyChanged";
    emit storagePropertyChanged(functionItem);
  }
  if (changed) {
    if (verbose) qDebug() << "Emitting dataChanged";
    QModelIndex topLeft(functionItem->index(this, 0));
    QModelIndex bottomRight(functionItem->index(this, GraphModel::NumColumns - 1));
    emit dataChanged(topLeft, bottomRight);
  }
  if (changed & WORKER_CHANGED) {
    emit workerChanged(prevWorkerSign, QString());
  }
}

void GraphModel::setProgramProperty(ProgramItem *, ParsedKey const &, std::shared_ptr<conf::Value const>)
{
}

void GraphModel::delProgramProperty(ProgramItem *, ParsedKey const &)
{
}

void GraphModel::setSiteProperty(
  SiteItem *siteItem, ParsedKey const &pk, std::shared_ptr<conf::Value const> v)
{
  if (pk.property == "is_master") {
    std::shared_ptr<Site> site =
      std::static_pointer_cast<Site>(siteItem->shared);

    std::shared_ptr<conf::RamenValueValue const> rv =
      std::dynamic_pointer_cast<conf::RamenValueValue const>(v);

    if (rv) {
      std::shared_ptr<VBool const> v =
        std::dynamic_pointer_cast<VBool const>(rv->v);
      if (v) {
        site->isMaster = v->v;
        /* Signal that the name has changed, although it's still TODO */
        QModelIndex index(siteItem->index(this, 0));
        emit dataChanged(index, index, { Qt::DisplayRole });
      }
    }
  }
}

void GraphModel::delSiteProperty(SiteItem *siteItem, ParsedKey const &pk)
{
  if (pk.property == "is_master") {
    std::shared_ptr<Site> site =
      std::static_pointer_cast<Site>(siteItem->shared);

    site->isMaster = false;
  }

  QModelIndex index(siteItem->index(this, 0));
  emit dataChanged(index, index, { Qt::DisplayRole });
}

void GraphModel::updateKey(std::string const &key, KValue const &kv)
{
  ParsedKey pk(key);
  if (! pk.valid) return;

  if (verbose)
    qDebug() << "GraphModel key" << QString::fromStdString(key) << "set to value "
             << *kv.val << "is valid:" << pk.valid;

  assert(pk.site.length() > 0);

  SiteItem *siteItem = nullptr;
  for (SiteItem *si : sites) {
    if (si->shared->name == pk.site) {
      siteItem = si;
      break;
    }
  }

  if (! siteItem) {
    if (verbose)
      qDebug() << "Creating a new Site" << pk.site;

    siteItem = new SiteItem(nullptr, std::make_unique<Site>(pk.site), settings);
    int idx = sites.size(); // as we insert at the end for now
    beginInsertRows(QModelIndex(), idx, idx);
    sites.insert(sites.begin()+idx, siteItem);
    reorder();
    endInsertRows();
  }

  if (pk.program.length() > 0) {
    ProgramItem *programItem = nullptr;
    for (ProgramItem *pi : siteItem->programs) {
      if (pi->shared->name == pk.program) {
        programItem = pi;
        break;
      }
    }
    if (! programItem) {
      if (verbose)
        qDebug() << "Creating a new Program" << pk.program;

      programItem =
        new ProgramItem(siteItem, std::make_unique<Program>(pk.program), settings);
      int idx = siteItem->programs.size();
      QModelIndex parent =
        createIndex(siteItem->row, 0, static_cast<GraphItem *>(siteItem));
      beginInsertRows(parent, idx, idx);
      siteItem->programs.insert(siteItem->programs.begin()+idx, programItem);
      siteItem->reorder(this);
      endInsertRows();
    }

    if (pk.function.length() > 0) {
      FunctionItem *functionItem = nullptr;
      for (FunctionItem *fi : programItem->functions) {
        if (fi->shared->name == pk.function) {
          functionItem = fi;
          break;
        }
      }
      if (! functionItem) {
        if (verbose)
          qDebug() << "Creating a new Function" << pk.function;

        QString const fqName(programItem->fqName() + "/" + pk.function);
        std::string srcPath(srcPathFromProgramName(
          programItem->shared->name.toStdString()));
        functionItem =
          new FunctionItem(
            programItem,
            std::make_unique<Function>(
              siteItem->shared->name, programItem->shared->name, pk.function, srcPath),
            settings);
        int idx = programItem->functions.size();
        QModelIndex parent =
          createIndex(programItem->row, 0, static_cast<GraphItem *>(programItem));
        beginInsertRows(parent, idx, idx);
        programItem->functions.insert(programItem->functions.begin()+idx, functionItem);
        programItem->reorder(this);
        endInsertRows();
        /* Since we have a new function, maybe we can solve some of the
         * pendingAddParents? */
        retryAddParents();
        emit functionAdded(functionItem);
      }
      setFunctionProperty(siteItem, programItem, functionItem, pk, kv.val);
    } else {
      setProgramProperty(programItem, pk, kv.val);
    }
  } else {
    setSiteProperty(siteItem, pk, kv.val);
  }
}

void GraphModel::deleteKey(std::string const &key, KValue const &)
{
  ParsedKey pk(key);
  if (! pk.valid) return;

  if (verbose)
    qDebug() << "GraphModel key" << QString::fromStdString(key) << "deleted, is valid:"
             << pk.valid;

  assert(pk.site.length() > 0);

  SiteItem *siteItem = nullptr;
  for (SiteItem *si : sites) {
    if (si->shared->name == pk.site) {
      siteItem = si;
      break;
    }
  }
  if (! siteItem) return;

  if (pk.program.length() > 0) {
    ProgramItem *programItem = nullptr;
    for (ProgramItem *pi : siteItem->programs) {
      if (pi->shared->name == pk.program) {
        programItem = pi;
        break;
      }
    }
    if (! programItem) return;

    if (pk.function.length() > 0) {
      FunctionItem *functionItem = nullptr;
      for (FunctionItem *fi : programItem->functions) {
        if (fi->shared->name == pk.function) {
          functionItem = fi;
          break;
        }
      }
      if (! functionItem) return;

      delFunctionProperty(functionItem, pk);
    } else {
      delProgramProperty(programItem, pk);
    }
  } else {
    delSiteProperty(siteItem, pk);
  }
}
