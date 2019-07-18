#include <QDateTime>
#include "GraphView.h"
#include "conf.h"
#include "TailModel.h"
#include "RamenType.h"
#include "FunctionItem.h"

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
      connect(kv, &KValue::valueCreated, this, &FunctionItem::addTuple);
  });
}

FunctionItem::~FunctionItem()
{
  if (tailModel) delete tailModel;
  for (RamenValue const *t : tuples) delete t;
}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

std::vector<std::pair<QString const, QString const>> FunctionItem::labels() const
{
  std::vector<std::pair<QString const, QString const>> labels;
  labels.reserve(8);

  if (worker && !worker->used)
    labels.emplace_back("", "UNUSED");
  if (firstStartupTime) {
    labels.emplace_back("first startup", stringOfDate(*firstStartupTime));
  }
  if (lastStartupTime) {
    labels.emplace_back("last startup", stringOfDate(*lastStartupTime));
  }
  if (eventTimeMin) {
    labels.emplace_back("min e-time", stringOfDate(*eventTimeMin));
  }
  if (eventTimeMax) {
    labels.emplace_back("max e-time", stringOfDate(*eventTimeMax));
  }
  if (totalTuples)
    labels.emplace_back("#tuples", QString::number(*totalTuples));
  if (totalBytes)
    labels.emplace_back("#bytes", QString::number(*totalBytes));
  if (totalCpu)
    labels.emplace_back("tot CPU", QString::number(*totalCpu));
  if (maxRAM)
    labels.emplace_back("max RAM", QString::number(*maxRAM));
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

  std::shared_ptr<conf::SourceInfo const> info =
    std::dynamic_pointer_cast<conf::SourceInfo const>(kv.val);
  if (! info) {
    std::cerr << k << " is not a SourceInfo" << std::endl;
    return nullptr;
  }
  if (info->errMsg.length() > 0) {
    std::cerr << k << " is not compiled" << std::endl;
    return nullptr;
  }
  for (CompiledFunctionInfo const *func : info->infos) {
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
    std::cout << "Received a tuple that was not a tuple: " << v << std::endl;
    return;
  }
  std::shared_ptr<RamenType const> type = outType();
  if (! type) { // ignore the tuple
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
