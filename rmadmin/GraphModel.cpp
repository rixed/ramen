#include <cassert>
#include <iostream>
#include <list>
#include <QRegularExpression>
#include "GraphModel.h"
#include "confValue.h"
#include "conf.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "SiteItem.h"

static bool const verbose = true;

GraphModel *GraphModel::globalGraphModel;

GraphModel::GraphModel(GraphViewSettings const *settings_, QObject *parent) :
  QAbstractItemModel(parent),
  settings(settings_)
{
  connect(&kvs, &KVStore::valueCreated,
          this, &GraphModel::updateKey);
  connect(&kvs, &KVStore::valueChanged,
          this, &GraphModel::updateKey);
  connect(&kvs, &KVStore::valueDeleted,
          this, &GraphModel::deleteKey);
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

  GraphItem *item =
    static_cast<GraphItem *>(index.internalPointer());
  return item->data(index.column(), role);
}

QString const GraphModel::columnName(GraphModel::Columns c)
{
  switch (c) {
    case Name: return tr("Name");
    case ActionButton: return QString();
    case WorkerTopHalf: return tr("Top-half");
    case WorkerEnabled: return tr("Enabled");
    case WorkerDebug: return tr("Debug");
    case WorkerUsed: return tr("Used");
    case StatsTime: return tr("Stats Emission");
    case StatsNumInputs: return tr("Inputs Events");
    case StatsNumSelected: return tr("Selected Events");
    case StatsTotWaitIn: return tr("Waiting for Input");
    case StatsTotInputBytes: return tr("Input Bytes");
    case StatsFirstInput: return tr("First Input Reception");
    case StatsLastInput: return tr("Last Input Reception");
    case StatsNumGroups: return tr("Groups");
    case StatsNumOutputs: return tr("Output Events");
    case StatsTotWaitOut: return tr("Waiting for Output");
    case StatsFirstOutput: return tr("First Output Emitted");
    case StatsLastOutput: return tr("Last Output Emitted");
    case StatsTotOutputBytes: return tr("Output Bytes");
    case StatsNumFiringNotifs: return tr("Fired Notifications");
    case StatsNumExtinguishedNotifs: return tr("Extinguished Notifications");
    case NumArcFiles: return tr("Archived Files");
    case NumArcBytes: return tr("Archived Bytes");
    case AllocedArcBytes: return tr("Allocated Archive Bytes");
    case StatsMinEventTime: return tr("Min. Event Time");
    case StatsMaxEventTime: return tr("Max. Event Time");
    case StatsTotCpu: return tr("Total CPU");
    case StatsCurrentRam: return tr("Current RAM");
    case StatsMaxRam: return tr("Max. RAM");
    case StatsFirstStartup: return tr("First Startup");
    case StatsLastStartup: return tr("Last Startup");
    case StatsAverageTupleSize: return tr("Average Bytes per Archived Event");
    case StatsNumAverageTupleSizeSamples: return tr("Full Event Size Samples");
    case WorkerReportPeriod: return tr("Report Period");
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

FunctionItem const *GraphModel::find(QString const &site, QString const &program, QString const &function)
{
  if (verbose)
    std::cout << "Look for function " << site.toStdString() << "/"
                                      << program.toStdString() << "/"
                                      << function.toStdString() << std::endl;
  for (SiteItem const *siteItem : sites) {
    if (siteItem->shared->name == site) {
      for (ProgramItem const *programItem : siteItem->programs) {
        if (programItem->shared->name == program) {
          for (FunctionItem const *functionItem : programItem->functions) {
            if (functionItem->shared->name == function) {
              return functionItem;
            }
          }
          if (verbose)
            std::cout << "No such function: " << function.toStdString() << std::endl;
          return nullptr;
        }
      }
      if (verbose)
        std::cout << "No such program: " << program.toStdString() << std::endl;
      return nullptr;
    }
  }
  if (verbose)
    std::cout << "No such site: " << site.toStdString() << std::endl;
  return nullptr;
}

void GraphModel::addFunctionParent(FunctionItem const *parent, FunctionItem *child)
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
    std::cout << "Will wait for parent before connecting to it" << std::endl;
  pendingAddParents.emplace_back(child, site, program, function);
}

void GraphModel::retryAddParents()
{
  for (auto it = pendingAddParents.begin(); it != pendingAddParents.end(); ) {
    FunctionItem const *parent = find(it->site, it->program, it->function);
    if (parent) {
      if (verbose)
        std::cout << "Resolved pending parent" << std::endl;
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
    std::cout << "setFunctionProperty for " << pk.property.toStdString() << std::endl;

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
        std::cout << "Ignoring new instance for "
                  << pk.instanceSignature.toStdString() << " because "
                  << (! function->worker ? "no worker yet" :
                        "its signature does not match worker's")
                  << std::endl;
    } else {
      if (function->instanceSignature.has_value() &&
          *function->instanceSignature != pk.instanceSignature) {
        if (verbose)
          std::cout << "Resetting old instance from "
                    << function->instanceSignature->toStdString() << " to "
                    << pk.instanceSignature.toStdString() << std::endl;
        function->resetInstanceData();
        changed |= PROPERTY_CHANGED + STORAGE_CHANGED;
      }
      function->instanceSignature = pk.instanceSignature;
      changed |= PROPERTY_CHANGED;
    }
  }

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
        std::cout << "Setting worker to "
                  << function->worker->workerSign.toStdString() << std::endl;

      for (auto ref : cf->parent_refs) {
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
        FunctionItem const *parent = find(psite, pprog, pfunc);
        if (parent) {
          if (verbose) std::cout << "Set immediate parent" << std::endl;
          addFunctionParent(parent, functionItem);
        } else {
          if (verbose) std::cout << "Set delayed parent" << std::endl;
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
    /* Worker did not really change, but everything that require the process list
     * to be invalidated must emit that signal: */
    SET_RAMENVALUE(VI64, pid, PROPERTY_CHANGED | WORKER_CHANGED);
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
      std::cout << "Useless property " << pk.property.toStdString() << std::endl;
  }

  if (changed & STORAGE_CHANGED) {
    if (verbose) std::cout << "Emitting storagePropertyChanged" << std::endl;
    emit storagePropertyChanged(functionItem);
  }
  if (changed & PROPERTY_CHANGED) {
    if (verbose) std::cout << "Emitting dataChanged" << std::endl;
    QModelIndex topLeft(functionItem->index(this, 0));
    QModelIndex bottomRight(functionItem->index(this, GraphModel::NumColumns - 1));
    emit dataChanged(topLeft, bottomRight, { Qt::DisplayRole });
  }
  if (changed & WORKER_CHANGED) {
    function->checkTail();
    emit workerChanged();
  }
}

void GraphModel::delFunctionProperty(
  FunctionItem *functionItem, ParsedKey const &pk)
{
  if (verbose)
    std::cout << "delFunctionProperty for " << pk.property.toStdString() << std::endl;

  int changed(0);

  std::shared_ptr<Function> function =
    std::static_pointer_cast<Function>(functionItem->shared);

  if (pk.property == "worker") {
    if (function->worker) {
      /* As we have connected this function to its parents (not treeParents!)
       * when the worker was received, disconnect it now: */
      removeParents(functionItem);
      changed |= STORAGE_CHANGED;
      if (verbose)
        std::cout << "Resetting worker "
                  << function->worker->workerSign.toStdString() << std::endl;
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
    if (verbose) std::cout << "Emitting storagePropertyChanged" << std::endl;
    emit storagePropertyChanged(functionItem);
  }
  if (changed) {
    if (verbose) std::cout << "Emitting dataChanged" << std::endl;
    QModelIndex topLeft(functionItem->index(this, 0));
    QModelIndex bottomRight(functionItem->index(this, GraphModel::NumColumns - 1));
    emit dataChanged(topLeft, bottomRight, { Qt::DisplayRole });
  }
  if (changed & WORKER_CHANGED) {
    emit workerChanged();
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

void GraphModel::updateKey(KVPair const &kvp)
{
  ParsedKey pk(kvp.first);
  if (! pk.valid) return;

  if (verbose)
    std::cout << "GraphModel key " << kvp.first << " set to value "
              << *kvp.second.val << " is valid:" << pk.valid << std::endl;

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
      std::cout << "Creating a new Site " << pk.site.toStdString() << std::endl;

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
        std::cout << "Creating a new Program " << pk.program.toStdString()
                  << std::endl;

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
          std::cout << "Creating a new Function " << pk.function.toStdString()
                    << std::endl;

        QString const programName(programItem->fqName());
        QString const fqName(programName + "/" + pk.function);
        std::string srcPath(srcPathFromProgramName(programName.toStdString()));
        functionItem =
          new FunctionItem(
            programItem,
            std::make_unique<Function>(pk.function, fqName, srcPath),
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
      setFunctionProperty(siteItem, programItem, functionItem, pk, kvp.second.val);
    } else {
      setProgramProperty(programItem, pk, kvp.second.val);
    }
  } else {
    setSiteProperty(siteItem, pk, kvp.second.val);
  }
}

void GraphModel::deleteKey(KVPair const &kvp)
{
  ParsedKey pk(kvp.first);
  if (! pk.valid) return;

  if (verbose)
    std::cout << "GraphModel key " << kvp.first << " deleted, is valid:"
              << pk.valid << std::endl;

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

std::ostream &operator<<(std::ostream &os, SiteItem const &s)
{
  os << "Site[" << s.row << "]:" << s.shared->name.toStdString() << std::endl;
  for (ProgramItem const *program : s.programs) {
    os << *program << std::endl;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, ProgramItem const &p)
{
  os << "  Program[" << p.row << "]:" << p.shared->name.toStdString() << std::endl;
  for (FunctionItem const *function : p.functions) {
    os << *function << std::endl;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, FunctionItem const &f)
{
  os << "    Function[" << f.row << "]:" << f.shared->name.toStdString();
  return os;
}
