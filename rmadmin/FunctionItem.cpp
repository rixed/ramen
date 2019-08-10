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

static std::string lastTuplesKey(FunctionItem const *f)
{
  return "^tails/" + f->fqName().toStdString() + "/lasts/";
}

FunctionItem::FunctionItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *settings) :
  GraphItem(treeParent, name, settings),
  tailModel(nullptr)
{
  // TODO: updateArrows should reallocate the channels:
  channel = std::rand() % settings->numArrowChannels;
  setZValue(3);
  tuples.reserve(1000);
  std::string k = lastTuplesKey(this);
  conf::autoconnect(k, [this](conf::Key const &, KValue const *kv) {
    // Although this value will never change we need the create signal:
      Once::connect(kv, &KValue::valueCreated, this, &FunctionItem::addTuple);
  });
}

FunctionItem::~FunctionItem()
{
  if (tailModel) delete tailModel;
  for (RamenValue const *t : tuples) delete t;
}

/* columnCount is called to know the number of columns of the sub elements.
 * Functions have no sub-elements and Qt should know this. */
int FunctionItem::columnCount() const
{
  assert(!"FunctionItem::columnCount called!");
}

QVariant FunctionItem::data(int column, int role) const
{
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
      return name;

    case GraphModel::ActionButton:
      if (role == GraphModel::SortRole)
        return name;
      else
        return QVariant();

    case GraphModel::WorkerTopHalf:
      return QString(
        worker && worker->role ?
          (worker->role->isTopHalf ? "✓" : "") : "?");

    case GraphModel::WorkerEnabled:
      return QString(
        worker ? (worker->enabled ? "✓" : "") : "?");

    case GraphModel::WorkerDebug:
      return QString(
        worker ? (worker->debug ? "✓" : "") : "?");

    case GraphModel::WorkerUsed:
      return QString(
        worker ? (worker->used ? "✓" : "") : "?");

    case GraphModel::StatsTime:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->statsTime : 0.;
      else return runtimeStats ?
          stringOfDate(runtimeStats->statsTime) : na;

    case GraphModel::StatsNumInputs:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totInputTuples : (qulonglong)0;
      else return runtimeStats ?
        QString::number(runtimeStats->totInputTuples) : na;

    case GraphModel::StatsNumSelected:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totSelectedTuples : (qulonglong)0;
      else return runtimeStats ?
        QString::number(runtimeStats->totSelectedTuples) : na;

    case GraphModel::StatsTotWaitIn:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totWaitIn : 0.;
      else return runtimeStats ?
        stringOfDuration(runtimeStats->totWaitIn) : na;

    case GraphModel::StatsTotInputBytes:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totInputBytes : (qulonglong)0;
      else return runtimeStats ?
        stringOfBytes(runtimeStats->totInputBytes) : na;

    case GraphModel::StatsFirstInput:
      if (role == GraphModel::SortRole)
        return runtimeStats && runtimeStats->firstInput.has_value() ?
          *runtimeStats->firstInput : 0.;
      else return runtimeStats && runtimeStats->firstInput.has_value() ?
        stringOfDate(*runtimeStats->firstInput) : na;

    case GraphModel::StatsLastInput:
      if (role == GraphModel::SortRole)
        return runtimeStats && runtimeStats->lastInput.has_value() ?
          *runtimeStats->lastInput : 0.;
      else return runtimeStats && runtimeStats->lastInput.has_value() ?
        stringOfDate(*runtimeStats->lastInput) : na;

    case GraphModel::StatsNumGroups:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->curGroups : (qulonglong)0;
      else return runtimeStats ?
        QString::number(runtimeStats->curGroups) : na;

    case GraphModel::StatsNumOutputs:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totOutputTuples : (qulonglong)0;
      else return runtimeStats ?
        QString::number(runtimeStats->totOutputTuples) : na;

    case GraphModel::StatsTotWaitOut:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totWaitOut : 0.;
      else return runtimeStats ?
        stringOfDuration(runtimeStats->totWaitOut) : na;

    case GraphModel::StatsFirstOutput:
      if (role == GraphModel::SortRole)
        return runtimeStats && runtimeStats->firstOutput.has_value() ?
          *runtimeStats->firstOutput : 0.;
      else return runtimeStats && runtimeStats->firstOutput.has_value() ?
        stringOfDate(*runtimeStats->firstOutput) : na;

    case GraphModel::StatsLastOutput:
      if (role == GraphModel::SortRole)
        return runtimeStats && runtimeStats->lastOutput.has_value() ?
          *runtimeStats->lastOutput : 0.;
      else return runtimeStats && runtimeStats->lastOutput.has_value() ?
        stringOfDate(*runtimeStats->lastOutput) : na;

    case GraphModel::StatsTotOutputBytes:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totOutputBytes : (qulonglong)0;
      else return runtimeStats ?
        stringOfBytes(runtimeStats->totOutputBytes) : na;

    case GraphModel::StatsNumFiringNotifs:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totFiringNotifs : (qulonglong)0;
      else return runtimeStats ?
        QString::number(runtimeStats->totFiringNotifs) : na;

    case GraphModel::StatsNumExtinguishedNotifs:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totExtinguishedNotifs : 0.;
      else return runtimeStats ?
        QString::number(runtimeStats->totExtinguishedNotifs) : na;

    case GraphModel::NumArcFiles:
      if (role == GraphModel::SortRole)
        return numArcFiles.has_value() ? *numArcFiles : (qulonglong)0;
      else return numArcFiles.has_value() ?
        QString::number(*numArcFiles) : na;

    case GraphModel::NumArcBytes:
      if (role == GraphModel::SortRole)
        return numArcBytes.has_value() ? *numArcBytes : (qulonglong)0;
      else return numArcBytes.has_value() ?
        stringOfBytes(*numArcBytes) : na;

    case GraphModel::AllocedArcBytes:
      if (role == GraphModel::SortRole)
        return allocArcBytes.has_value() ? *allocArcBytes : (qulonglong)0;
      else return allocArcBytes.has_value() ?
        stringOfBytes(*allocArcBytes) : na;

    case GraphModel::StatsMinEventTime:
      if (role == GraphModel::SortRole)
        return runtimeStats && runtimeStats->minEventTime.has_value() ?
          *runtimeStats->minEventTime : 0.;
      else return runtimeStats && runtimeStats->minEventTime.has_value() ?
        stringOfDate(*runtimeStats->minEventTime) : na;

    case GraphModel::StatsMaxEventTime:
      if (role == GraphModel::SortRole)
        return runtimeStats && runtimeStats->maxEventTime.has_value() ?
          *runtimeStats->maxEventTime : 0.;
      else return runtimeStats && runtimeStats->maxEventTime.has_value() ?
        stringOfDate(*runtimeStats->maxEventTime) : na;

    case GraphModel::StatsTotCpu:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totCpu : 0.;
      else return runtimeStats ?
        stringOfDuration(runtimeStats->totCpu) : na;

    case GraphModel::StatsCurrentRam:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->curRam : (qulonglong)0;
      else return runtimeStats ?
        stringOfBytes(runtimeStats->curRam) : na;

    case GraphModel::StatsMaxRam:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->maxRam : (qulonglong)0;
      else return runtimeStats ?
        stringOfBytes(runtimeStats->maxRam) : na;

    case GraphModel::StatsFirstStartup:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->firstStartup : 0.;
      else return runtimeStats ?
        stringOfDate(runtimeStats->firstStartup) : na;

    case GraphModel::StatsLastStartup:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->lastStartup : 0.;
      else return runtimeStats ?
        stringOfDate(runtimeStats->lastStartup) : na;

    case GraphModel::StatsAverageTupleSize:
      if (runtimeStats && runtimeStats->totFullBytesSamples > 0) {
        double const avg =
          runtimeStats->totFullBytes / runtimeStats->totFullBytesSamples;
        if (role == GraphModel::SortRole) return avg;
        else return stringOfBytes(avg);
      } else {
        if (role == GraphModel::SortRole) return 0.;
        else return na;
      }

    case GraphModel::StatsNumAverageTupleSizeSamples:
      if (role == GraphModel::SortRole)
        return runtimeStats ? runtimeStats->totFullBytesSamples : (qulonglong)0;
      else return runtimeStats ?
        QString::number(runtimeStats->totFullBytesSamples) : na;

    case GraphModel::WorkerReportPeriod:
      if (role == GraphModel::SortRole)
        return worker ? worker->reportPeriod : 0.;
      else return worker ?
        stringOfDuration(worker->reportPeriod) : na;

    case GraphModel::WorkerSrcPath:
      return worker ? worker->srcPath : na;

    case GraphModel::WorkerParams:
      if (worker) {
        QString v;
        for (auto &p : worker->params) {
          if (v.length() > 0) v.append(", ");
          v.append(p->toQString());
        }
        return v;
      } else return na;

    case GraphModel::NumParents:
      if (role == GraphModel::SortRole)
        return (qulonglong)(worker ? worker->parent_refs.size() : 0);
      else return worker ?
        QString::number(worker->parent_refs.size()) : na;

    case GraphModel::NumChildren:
      return na;  // TODO

    case GraphModel::WorkerSignature:
      return worker ? worker->workerSign : na;

    case GraphModel::WorkerBinSignature:
      return worker ? worker->binSign : na;

    case GraphModel::NumTailTuples:
      if (role == GraphModel::SortRole) return (qulonglong)tuples.size();
      else return QString::number(tuples.size());

    case GraphModel::NumColumns:
      break;
  }

  assert(!"Bad columnCount for FunctionItem");
}

std::vector<std::pair<QString const, QString const>> FunctionItem::labels() const
{
  std::vector<std::pair<QString const, QString const>> labels;
  labels.reserve(8);

  if (worker && !worker->used)
    labels.emplace_back("", "UNUSED");
  // TODO: display some stats
  if (numArcFiles)
    labels.emplace_back("#arc.files", QString::number(*numArcFiles));
  if (numArcBytes)
    labels.emplace_back("arc.size", QString::number(*numArcBytes));
  if (allocArcBytes)
    labels.emplace_back("arc.allocated", QString::number(*allocArcBytes));

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

int FunctionItem::numRows() const
{
  return tuples.size();
}

/* Look for in in the kvs at every call rather than caching a value that
 * could change at any time. */
CompiledFunctionInfo const *FunctionItem::compiledInfo() const
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

std::shared_ptr<RamenType const> FunctionItem::outType() const
{
  CompiledFunctionInfo const *func = compiledInfo();
  if (! func) return nullptr;

  return func->out_type;
}

int FunctionItem::numColumns() const
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

// Returned value owned by FunctionItem:
RamenValue const *FunctionItem::tupleData(int row, int column) const
{
  if (row >= numRows()) return nullptr;

  return tuples[row]->columnValue(column);
}

QString FunctionItem::header(unsigned column) const
{
  std::shared_ptr<RamenType const> t = outType();
  if (! t) return QString("#") + QString::number(column);

  return t->structure->columnName(column);
}

void FunctionItem::addTuple(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::Tuple const> tuple =
    std::dynamic_pointer_cast<conf::Tuple const>(v);
  if (! tuple) {
    if (verbose)
      std::cout << "Received a tuple that was not a tuple: " << v << std::endl;
    return;
  }
  std::shared_ptr<RamenType const> type = outType();
  if (! type) { // ignore the tuple
    if (verbose)
      std::cout << "Received a tuple for " << fqName().toStdString()
                << " before we know the type" << std::endl;
    return;
  }

  RamenValue const *val = tuple->unserialize(type);
  if (! val) return;

  emit beginAddTuple(QModelIndex(), tuples.size(), tuples.size());
  tuples.push_back(val);
  emit endAddTuple();
}

bool FunctionItem::isTopHalf() const
{
  return worker && worker->role && worker->role->isTopHalf;
}

bool FunctionItem::isWorking() const
{
  return worker != nullptr;
}
