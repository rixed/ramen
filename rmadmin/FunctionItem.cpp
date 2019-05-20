#include <QDateTime>
#include "GraphView.h"
#include "conf.h"
#include "TailModel.h"
#include "FunctionItem.h"

static std::string lastTuplesKey(FunctionItem const *f)
{
  return "tail/" + f->fqName().toStdString() + "/lasts/";
}

FunctionItem::FunctionItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *settings, unsigned paletteSize) :
  GraphItem(treeParent, name, settings, paletteSize)
{
  // TODO: updateArrows should reallocate the channels:
  channel = std::rand() % settings->numArrowChannels;
  setZValue(3);
  tuples.reserve(1000);
  std::string k = lastTuplesKey(this);
  conf::autoconnect(k, [this](conf::Key const &, KValue const *kv) {
    // Although this value will never change we need the create signal:
      QObject::connect(kv, &KValue::valueCreated, this, &FunctionItem::addTuple);
  });
  tailModel = new TailModel(this);
}

FunctionItem::~FunctionItem()
{
  delete tailModel;
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

  if (isUsed && !(*isUsed))
    labels.emplace_back("", "UNUSED");
  if (startupTime) {
    QDateTime dt = QDateTime::fromSecsSinceEpoch(*startupTime);
    labels.emplace_back("startup", dt.toString());
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

int FunctionItem::numColumns() const
{
  // We could get this info from already received tuples or from the
  // output type in the config tree.
  // We go for the output type as it also gives us column headers:
  // TODO: a function to get it and cache it. Or even better: connect
  // to this KV set/change signals and update the cached type.
  conf::Key k("programs/" + treeParent->name.toStdString() + "/functions/" +
              name.toStdString() + "/type/out");
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[k];
  conf::kvs_lock.unlock_shared();

  std::shared_ptr<conf::RamenType const> outType =
    std::dynamic_pointer_cast<conf::RamenType const>(kv.value());
  if (! outType) return 0;

  return outType->numColumns();
}

QString FunctionItem::header(unsigned column) const
{
  // TODO: see above
  conf::Key k("programs/" + treeParent->name.toStdString() + "/functions/" +
              name.toStdString() + "/type/out");
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[k];
  conf::kvs_lock.unlock_shared();

  std::shared_ptr<conf::RamenType const> outType =
    std::dynamic_pointer_cast<conf::RamenType const>(kv.value());
  if (! outType) return QString("#") + QString::number(column);

  return outType->header(column);
}

void FunctionItem::addTuple(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::Tuple const> t =
    std::dynamic_pointer_cast<conf::Tuple const>(v);
  if (! t) {
    std::cout << "Received a tuple that was not a tuple: " << v << std::endl;
    return;
  }
  // TODO: in theory check the seq number and ensure ordering:
  emit beginAddTuple(tuples.size(), tuples.size()+1);
  tuples.push_back(t);
  emit endAddTuple();
}
