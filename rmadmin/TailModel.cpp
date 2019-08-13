#include <cassert>
#include <string>
#include <memory>
#include "once.h"
#include "conf.h"
#include "confKey.h"
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
  type(type_),
  factors(factors_)
{
  tuples.reserve(500);

  // Get prepared to record tuples from the tail, then subscribe:
  std::string const lasts("^tails/" + fqName.toStdString() + "/" +
                          workerSign.toStdString() + "/lasts/");
  conf::autoconnect(lasts, [this](conf::Key const &, KValue const *kv) {
    Once::connect(kv, &KValue::valueCreated,
                  this, &TailModel::addTuple);
  });

  // Subscribe
  conf::Key k(subscriberKey());
  // TODO: have a VoidType
  std::shared_ptr<conf::Value> v(new conf::RamenValueValue(new VBool(true)));
  conf::askSet(k, v);
}

TailModel::~TailModel()
{
  // Unsubscribe
  conf::Key k(subscriberKey());
  conf::askDel(k);
}

conf::Key TailModel::subscriberKey() const
{
  return conf::Key(
    "tails/" + fqName.toStdString() + "/" +
    workerSign.toStdString() + "/users/" + my_uid->toStdString());
}

void TailModel::addTuple(conf::Key const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::Tuple const> tuple =
    std::dynamic_pointer_cast<conf::Tuple const>(v);
  if (! tuple) {
    std::cerr << "Received a tuple that was not a tuple: " << v << std::endl;
    return;
  }

  RamenValue const *val = tuple->unserialize(type);
  if (! val) {
    std::cerr << "Cannot unserialize tuple: " << v << std::endl;
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
        QVariant(tuples[row]->columnValue(column)->toQString(conf::Key::null));
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
