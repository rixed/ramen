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
  for (ser::Value const *t : tuples) delete t;
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
    QDateTime dt = QDateTime::fromSecsSinceEpoch(*firstStartupTime);
    labels.emplace_back("first startup", dt.toString());
  }
  if (lastStartupTime) {
    QDateTime dt = QDateTime::fromSecsSinceEpoch(*lastStartupTime);
    labels.emplace_back("last startup", dt.toString());
  }
  if (eventTimeMin) {
    QDateTime dt = QDateTime::fromSecsSinceEpoch(*eventTimeMin);
    labels.emplace_back("min e-time", dt.toString());
  }
  if (eventTimeMax) {
    QDateTime dt = QDateTime::fromSecsSinceEpoch(*eventTimeMax);
    labels.emplace_back("max e-time", dt.toString());
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

conf::Key FunctionItem::functionKey(std::string perFuncKey) const
{
  return conf::Key("programs/" + treeParent->name.toStdString() + "/functions/" +
                   name.toStdString() + perFuncKey);
}

std::shared_ptr<RamenType const> FunctionItem::outType() const
{
  conf::Key k = functionKey("/type/out");
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[k];
  conf::kvs_lock.unlock_shared();

  std::shared_ptr<RamenType const> outType =
    std::dynamic_pointer_cast<RamenType const>(kv.val);
  return outType;
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
  return t->numColumns();
}

// Returned value owned by FunctionItem:
ser::Value const *FunctionItem::tupleData(int row, int column) const
{
  if (row >= numRows()) return nullptr;

  return tuples[row]->columnValue(column);
}

QString FunctionItem::header(unsigned column) const
{
  std::shared_ptr<RamenType const> t = outType();
  if (! t) return QString("#") + QString::number(column);

  return t->columnName(column);
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

  ser::Value const *val = tuple->unserialize(type);
  if (! val) return;

  emit beginAddTuple(QModelIndex(), tuples.size(), tuples.size());
  tuples.push_back(val);
  emit endAddTuple();
}
