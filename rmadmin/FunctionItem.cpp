#include <QDateTime>
#include "once.h"
#include "misc.h"
#include "GraphView.h"
#include "conf.h"
#include "TailModel.h"
#include "RamenType.h"
#include "confWorkerRole.h"
#include "confRCEntryParam.h"
#include "Resources.h"
#include "FunctionItem.h"

static bool verbose = false;

Function::~Function()
{
  if (tailModel) delete tailModel;
}

QString Function::header(unsigned column) const
{
  std::shared_ptr<RamenType const> t = outType();
  if (! t) return QString("#") + QString::number(column);

  return t->structure->columnName(column);
}

/* Look for in in the kvs at every call rather than caching a value that
 * could change at any time. */
CompiledFunctionInfo const *Function::compiledInfo() const
{
  if (! worker) return nullptr;

  conf::Key k = "sources/" + worker->srcPath.toStdString() + "/info";
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[k];
  conf::kvs_lock.unlock_shared();

  if (! kv.val) {
    if (verbose) std::cout << k.s << " not yet set" << std::endl;
    return nullptr;
  }

  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(kv.val);
  if (! info) {
    std::cerr << k << " is not a SourceInfo but: " << kv.val << std::endl;
    return nullptr;
  }
  if (info->errMsg.length() > 0) {
    std::cerr << k << " is not compiled" << std::endl;
    return nullptr;
  }
  for (unsigned i = 0; i < info->infos.size(); i ++) {
    CompiledFunctionInfo const *func = &info->infos[i];
    if (func->name == name) return func;
  }

  return nullptr;
}

std::shared_ptr<RamenType const> Function::outType() const
{
  CompiledFunctionInfo const *func = compiledInfo();
  if (! func) return nullptr;

  return func->out_type;
}

int Function::numColumns() const
{
  // We could get this info from already received tuples or from the
  // output type in the config tree.
  // We go for the output type as it is constant, and model numColumns is now
  // allowed to change.
  // TODO: a function to get it and cache it. Or even better: connect
  // to this KV set/change signals and update the cached type.
  std::shared_ptr<RamenType const> t = outType();
  if (! t) return 0;
  return t->structure->numColumns();
}

void Function::resetInstanceData()
{
  pid.reset();
  lastKilled.reset();
  lastExit.reset();
  lastExitStatus.reset();
  successiveFailures.reset();
  quarantineUntil.reset();
  tuples.clear();
}

static std::string lastTuplesKey(FunctionItem const *f)
{
  return "^tails/" + f->fqName().toStdString() + "/lasts/";
}

FunctionItem::FunctionItem(
  GraphItem *treeParent, std::unique_ptr<Function> function,
  GraphViewSettings const *settings) :
  GraphItem(treeParent, std::move(function), settings)
{
  // TODO: updateArrows should reallocate the channels:
  channel = std::rand() % settings->numArrowChannels;
  setZValue(3);

  std::string k = lastTuplesKey(this);
  conf::autoconnect(k, [this](conf::Key const &, KValue const *kv) {
    // Although this value will never change we need the create signal:
      Once::connect(kv, &KValue::valueCreated, this, &FunctionItem::addTuple);
  });
}

/* columnCount is called to know the number of columns of the sub elements.
 * Functions have no sub-elements and Qt should know this. */
int FunctionItem::columnCount() const
{
  assert(!"FunctionItem::columnCount called!");
}

QVariant FunctionItem::data(int column, int role) const
{
  std::shared_ptr<Function> shr =
    std::static_pointer_cast<Function>(shared);

  if (role == Qt::DisplayRole &&
      !isTopHalf() &&
      column == GraphModel::ActionButton)
    return Resources::get()->tablePixmap;

  static QString na(tr("n.a"));

  if (role == Qt::TextAlignmentRole) {
    switch (column) {
      case GraphModel::Name:
        return Qt::AlignLeft;
      case GraphModel::WorkerTopHalf:
      case GraphModel::WorkerEnabled:
      case GraphModel::WorkerDebug:
      case GraphModel::WorkerUsed:
      case GraphModel::WorkerParams:
        return Qt::AlignHCenter;
    }
    return Qt::AlignRight;
  }

  if (role != Qt::DisplayRole &&
      role != GraphModel::SortRole) return QVariant();

  switch ((GraphModel::Columns)column) {
    case GraphModel::Name:
      return shr->name;

    case GraphModel::ActionButton:
      if (role == GraphModel::SortRole)
        return shr->name;
      else
        return QVariant();

    case GraphModel::WorkerTopHalf:
      return QString(
        shr->worker && shr->worker->role ?
          (shr->worker->role->isTopHalf ? "✓" : "") : "?");

    case GraphModel::WorkerEnabled:
      return QString(
        shr->worker ? (shr->worker->enabled ? "✓" : "") : "?");

    case GraphModel::WorkerDebug:
      return QString(
        shr->worker ? (shr->worker->debug ? "✓" : "") : "?");

    case GraphModel::WorkerUsed:
      return QString(
        shr->worker ? (shr->worker->used ? "✓" : "") : "?");

    case GraphModel::StatsTime:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ? shr->runtimeStats->statsTime : 0.;
      else return shr->runtimeStats ?
          stringOfDate(shr->runtimeStats->statsTime) : na;

    case GraphModel::StatsNumInputs:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->totInputTuples : (qulonglong)0;
      else return shr->runtimeStats ?
        QString::number(shr->runtimeStats->totInputTuples) : na;

    case GraphModel::StatsNumSelected:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->totSelectedTuples : (qulonglong)0;
      else return shr->runtimeStats ?
        QString::number(shr->runtimeStats->totSelectedTuples) : na;

    case GraphModel::StatsTotWaitIn:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ? shr->runtimeStats->totWaitIn : 0.;
      else return shr->runtimeStats ?
        stringOfDuration(shr->runtimeStats->totWaitIn) : na;

    case GraphModel::StatsTotInputBytes:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->totInputBytes : (qulonglong)0;
      else return shr->runtimeStats ?
        stringOfBytes(shr->runtimeStats->totInputBytes) : na;

    case GraphModel::StatsFirstInput:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats &&
               shr->runtimeStats->firstInput.has_value() ?
          *shr->runtimeStats->firstInput : 0.;
      else return shr->runtimeStats &&
                  shr->runtimeStats->firstInput.has_value() ?
        stringOfDate(*shr->runtimeStats->firstInput) : na;

    case GraphModel::StatsLastInput:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats &&
               shr->runtimeStats->lastInput.has_value() ?
          *shr->runtimeStats->lastInput : 0.;
      else return shr->runtimeStats &&
                  shr->runtimeStats->lastInput.has_value() ?
        stringOfDate(*shr->runtimeStats->lastInput) : na;

    case GraphModel::StatsNumGroups:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->curGroups : (qulonglong)0;
      else return shr->runtimeStats ?
        QString::number(shr->runtimeStats->curGroups) : na;

    case GraphModel::StatsNumOutputs:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->totOutputTuples : (qulonglong)0;
      else return shr->runtimeStats ?
        QString::number(shr->runtimeStats->totOutputTuples) : na;

    case GraphModel::StatsTotWaitOut:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->totWaitOut : 0.;
      else return shr->runtimeStats ?
        stringOfDuration(shr->runtimeStats->totWaitOut) : na;

    case GraphModel::StatsFirstOutput:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats &&
               shr->runtimeStats->firstOutput.has_value() ?
          *shr->runtimeStats->firstOutput : 0.;
      else return shr->runtimeStats &&
                  shr->runtimeStats->firstOutput.has_value() ?
        stringOfDate(*shr->runtimeStats->firstOutput) : na;

    case GraphModel::StatsLastOutput:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats &&
               shr->runtimeStats->lastOutput.has_value() ?
          *shr->runtimeStats->lastOutput : 0.;
      else return shr->runtimeStats &&
                  shr->runtimeStats->lastOutput.has_value() ?
        stringOfDate(*shr->runtimeStats->lastOutput) : na;

    case GraphModel::StatsTotOutputBytes:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->totOutputBytes : (qulonglong)0;
      else return shr->runtimeStats ?
        stringOfBytes(shr->runtimeStats->totOutputBytes) : na;

    case GraphModel::StatsNumFiringNotifs:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->totFiringNotifs : (qulonglong)0;
      else return shr->runtimeStats ?
        QString::number(shr->runtimeStats->totFiringNotifs) : na;

    case GraphModel::StatsNumExtinguishedNotifs:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->totExtinguishedNotifs : 0.;
      else return shr->runtimeStats ?
        QString::number(shr->runtimeStats->totExtinguishedNotifs) : na;

    case GraphModel::NumArcFiles:
      if (role == GraphModel::SortRole)
        return shr->numArcFiles.has_value() ?
          *shr->numArcFiles : (qulonglong)0;
      else return shr->numArcFiles.has_value() ?
        QString::number(*shr->numArcFiles) : na;

    case GraphModel::NumArcBytes:
      if (role == GraphModel::SortRole)
        return shr->numArcBytes.has_value() ?
          *shr->numArcBytes : (qulonglong)0;
      else return shr->numArcBytes.has_value() ?
        stringOfBytes(*shr->numArcBytes) : na;

    case GraphModel::AllocedArcBytes:
      if (role == GraphModel::SortRole)
        return shr->allocArcBytes.has_value() ?
          *shr->allocArcBytes : (qulonglong)0;
      else return shr->allocArcBytes.has_value() ?
        stringOfBytes(*shr->allocArcBytes) : na;

    case GraphModel::StatsMinEventTime:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats &&
               shr->runtimeStats->minEventTime.has_value() ?
          *shr->runtimeStats->minEventTime : 0.;
      else return shr->runtimeStats &&
                  shr->runtimeStats->minEventTime.has_value() ?
        stringOfDate(*shr->runtimeStats->minEventTime) : na;

    case GraphModel::StatsMaxEventTime:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats &&
               shr->runtimeStats->maxEventTime.has_value() ?
          *shr->runtimeStats->maxEventTime : 0.;
      else return shr->runtimeStats &&
                  shr->runtimeStats->maxEventTime.has_value() ?
        stringOfDate(*shr->runtimeStats->maxEventTime) : na;

    case GraphModel::StatsTotCpu:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ? shr->runtimeStats->totCpu : 0.;
      else return shr->runtimeStats ?
        stringOfDuration(shr->runtimeStats->totCpu) : na;

    case GraphModel::StatsCurrentRam:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->curRam : (qulonglong)0;
      else return shr->runtimeStats ?
        stringOfBytes(shr->runtimeStats->curRam) : na;

    case GraphModel::StatsMaxRam:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->maxRam : (qulonglong)0;
      else return shr->runtimeStats ?
        stringOfBytes(shr->runtimeStats->maxRam) : na;

    case GraphModel::StatsFirstStartup:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ? shr->runtimeStats->firstStartup : 0.;
      else return shr->runtimeStats ?
        stringOfDate(shr->runtimeStats->firstStartup) : na;

    case GraphModel::StatsLastStartup:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ? shr->runtimeStats->lastStartup : 0.;
      else return shr->runtimeStats ?
        stringOfDate(shr->runtimeStats->lastStartup) : na;

    case GraphModel::StatsAverageTupleSize:
      if (shr->runtimeStats &&
          shr->runtimeStats->totFullBytesSamples > 0)
      {
        double const avg =
          shr->runtimeStats->totFullBytes /
          shr->runtimeStats->totFullBytesSamples;
        if (role == GraphModel::SortRole) return avg;
        else return stringOfBytes(avg);
      } else {
        if (role == GraphModel::SortRole) return 0.;
        else return na;
      }

    case GraphModel::StatsNumAverageTupleSizeSamples:
      if (role == GraphModel::SortRole)
        return shr->runtimeStats ?
          shr->runtimeStats->totFullBytesSamples : (qulonglong)0;
      else return shr->runtimeStats ?
        QString::number(shr->runtimeStats->totFullBytesSamples) : na;

    case GraphModel::WorkerReportPeriod:
      if (role == GraphModel::SortRole)
        return shr->worker ? shr->worker->reportPeriod : 0.;
      else return shr->worker ?
        stringOfDuration(shr->worker->reportPeriod) : na;

    case GraphModel::WorkerSrcPath:
      return shr->worker ? shr->worker->srcPath : na;

    case GraphModel::WorkerParams:
      if (shr->worker) {
        QString v;
        for (auto &p : shr->worker->params) {
          if (v.length() > 0) v.append(", ");
          v.append(p->toQString());
        }
        return v;
      } else return na;

    case GraphModel::NumParents:
      if (role == GraphModel::SortRole)
        return (qulonglong)(shr->worker ?
          shr->worker->parent_refs.size() : 0);
      else return shr->worker ?
        QString::number(shr->worker->parent_refs.size()) : na;

    case GraphModel::NumChildren:
      return na;  // TODO

    case GraphModel::InstancePid:
      if (role == GraphModel::SortRole)
        return shr->pid.has_value() ? *shr->pid : (qulonglong)0;
      else
        return shr->pid.has_value() ? QString::number(*shr->pid) : na;

    case GraphModel::InstanceLastKilled:
      if (role == GraphModel::SortRole)
        return shr->lastKilled.has_value() ? *shr->lastKilled : 0.;
      else return shr->lastKilled.has_value() ?
        stringOfDate(*shr->lastKilled) : na;

    case GraphModel::InstanceLastExit:
      if (role == GraphModel::SortRole)
        return shr->lastExit.has_value() ? *shr->lastExit : 0.;
      else return shr->lastExit.has_value() ?
        stringOfDate(*shr->lastExit) : na;

    case GraphModel::InstanceLastExitStatus:
      if (role == GraphModel::SortRole)
        return shr->lastExitStatus.has_value() ? *shr->lastExitStatus : na;
      else
        return shr->lastExitStatus.has_value() ? *shr->lastExitStatus : na;

    case GraphModel::InstanceSuccessiveFailures:
      if (role == GraphModel::SortRole)
        return shr->successiveFailures.has_value() ?
          *shr->successiveFailures : (qulonglong)0;
      else
        return shr->successiveFailures.has_value() ?
          QString::number(*shr->successiveFailures) : na;

    case GraphModel::InstanceQuarantineUntil:
      if (role == GraphModel::SortRole)
        return shr->quarantineUntil.has_value() ? *shr->quarantineUntil : 0.;
      else return shr->quarantineUntil.has_value() ?
        stringOfDate(*shr->quarantineUntil) : na;

    case GraphModel::InstanceSignature:
      if (role == GraphModel::SortRole)
        return shr->instanceSignature.has_value() ?
          *shr->instanceSignature : na;
      else
        return shr->instanceSignature.has_value() ?
          *shr->instanceSignature : na;

    case GraphModel::WorkerSignature:
      return shr->worker ? shr->worker->workerSign : na;

    case GraphModel::WorkerBinSignature:
      return shr->worker ? shr->worker->binSign : na;

    case GraphModel::NumTailTuples:
      if (role == GraphModel::SortRole)
        return (qulonglong)shr->tuples.size();
      else return QString::number(shr->tuples.size());

    case GraphModel::NumColumns:
      break;
  }

  assert(!"Bad columnCount for FunctionItem");
}

std::vector<std::pair<QString const, QString const>> FunctionItem::labels() const
{
  std::shared_ptr<Function> shr =
    std::static_pointer_cast<Function>(shared);

  std::vector<std::pair<QString const, QString const>> labels;
  labels.reserve(8);

  if (shr->worker && !shr->worker->used)
    labels.emplace_back("", "UNUSED");
  // TODO: display some stats
  if (shr->numArcFiles)
    labels.emplace_back("#arc.files", QString::number(*shr->numArcFiles));
  if (shr->numArcBytes)
    labels.emplace_back("arc.size", QString::number(*shr->numArcBytes));
  if (shr->allocArcBytes)
    labels.emplace_back("arc.allocated", QString::number(*shr->allocArcBytes));

  return labels;
}

QRectF FunctionItem::operationRect() const
{
  return
    QRect(0, 0,
          settings->gridWidth - 2 * (
            settings->functionMarginHoriz +
            settings->programMarginHoriz +
            settings->siteMarginHoriz),
          settings->gridHeight - (
            settings->functionMarginBottom + settings->programMarginBottom +
            settings->siteMarginBottom + settings->functionMarginTop +
            settings->programMarginTop + settings->siteMarginTop));
}

void FunctionItem::addTuple(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<Function> shr =
    std::static_pointer_cast<Function>(shared);

  /* Reject tuples unless worker signature and instance signature agree.
   * In theory, before a new worker has a chance to get started the new
   * worker must have been set, and since messages are send in order we
   * must learn about the new worker before we receive any one of those
   * new tuples. */
  if (! shr->worker || ! shr->instanceSignature.has_value() ||
      shr->worker->workerSign != *shr->instanceSignature) {
    if (verbose)
      std::cout << "Rejecting tuple because worker and instance "
                   "signatures disagree" << std::endl;
    return;
  }

  std::shared_ptr<conf::Tuple const> tuple =
    std::dynamic_pointer_cast<conf::Tuple const>(v);
  if (! tuple) {
    if (verbose)
      std::cout << "Received a tuple that was not a tuple: " << v << std::endl;
    return;
  }

  std::shared_ptr<RamenType const> type = shr->outType();
  if (! type) { // ignore the tuple
    if (verbose)
      std::cout << "Received a tuple for " << fqName().toStdString()
                << " before we know the type" << std::endl;
    return;
  }

  RamenValue const *val = tuple->unserialize(type);
  if (! val) return;

  emit shr->beginAddTuple(
    QModelIndex(), shr->tuples.size(), shr->tuples.size());
  shr->tuples.emplace_back(val);
  emit shr->endAddTuple();
}

bool FunctionItem::isTopHalf() const
{
  std::shared_ptr<Function> shr =
    std::static_pointer_cast<Function>(shared);

  return shr->worker &&
         shr->worker->role &&
         shr->worker->role->isTopHalf;
}

bool FunctionItem::isWorking() const
{
  std::shared_ptr<Function> shr =
    std::static_pointer_cast<Function>(shared);

  return shr->worker != nullptr;
}
