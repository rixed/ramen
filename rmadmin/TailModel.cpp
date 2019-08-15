#include <cassert>
#include <string>
#include <memory>
#include "conf.h"
#include "confValue.h"
#include "RamenType.h"
#include "TailModel.h"

TailModel::TailModel(
  QString const &fqName_, QString const &workerSign_,
  std::shared_ptr<RamenType const> type_,
  QStringList factors_,
  QObject *parent) :
  QAbstractTableModel(parent),
  fqName(fqName_),
  workerSign(workerSign_),
  keyPrefix("tails/" + fqName.toStdString() + "/" +
            workerSign.toStdString() + "/lasts/"),
  type(type_),
  factors(factors_)
{
  tuples.reserve(500);

  connect(&kvs, &KVStore::valueCreated,
          this, &TailModel::addTuple);

  // Subscribe
  std::string k(subscriberKey());
  // TODO: have a VoidType
  std::shared_ptr<conf::Value> v(new conf::RamenValueValue(new VBool(true)));
  askSet(k, v);
}

TailModel::~TailModel()
{
  // Unsubscribe
  std::string k(subscriberKey());
  askDel(k);
}

std::string TailModel::subscriberKey() const
{
  return std::string(
    "tails/" + fqName.toStdString() + "/" +
    workerSign.toStdString() + "/users/" + my_uid->toStdString());
}

void TailModel::addTuple(KVPair const &kvp)
{
  if (! startsWith(kvp.first, keyPrefix)) return;

  std::shared_ptr<conf::Tuple const> tuple =
    std::dynamic_pointer_cast<conf::Tuple const>(kvp.second.val);
  if (! tuple) {
    std::cerr << "Received a tuple that was not a tuple: " << *kvp.second.val
              << std::endl;
    return;
  }

  RamenValue const *val = tuple->unserialize(type);
  if (! val) {
    std::cerr << "Cannot unserialize tuple: " << *kvp.second.val << std::endl;
    return;
  }

  beginInsertRows(QModelIndex(), tuples.size(), tuples.size());
  tuples.emplace_back(val);
  endInsertRows();
}

int TailModel::rowCount(QModelIndex const &parent) const
{
  if (parent.isValid()) return 0;
  return tuples.size();
}

int TailModel::columnCount(QModelIndex const &parent) const
{
  if (parent.isValid()) return 0;
  return type->structure->numColumns();
}

QVariant TailModel::data(QModelIndex const &index, int role) const
{
  if (! index.isValid()) return QVariant();

  int const row = index.row();
  int const column = index.column();

  if (row < 0 || row >= rowCount() ||
      column < 0 || column >= columnCount())
    return QVariant();

  switch (role) {
    case Qt::DisplayRole:
      return
        QVariant(tuples[row]->columnValue(column)->toQString(std::string()));
    case Qt::ToolTipRole:
      // TODO
      return QVariant(QString("Column #") + QString::number(column));
    default:
      return QVariant();
  }
}

QVariant TailModel::headerData(int section, Qt::Orientation orient, int role) const
{
  if (role != Qt::DisplayRole)
    return QAbstractTableModel::headerData(section, orient, role);

  switch (orient) {
    case Qt::Horizontal:
      if (section < 0 || section >= columnCount()) return QVariant();
      return type->structure->columnName(section);
    case Qt::Vertical:
      if (section < 0 || section >= rowCount()) return QVariant();
      return QVariant(QString::number(section));
  }
  return QVariant();
}
