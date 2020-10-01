#include <cassert>
#include <cmath>
#include <string>
#include <memory>
#include <QDebug>
#include <QtGlobal>
#include "conf.h"
#include "confValue.h"
#include "EventTime.h"
#include "RamenType.h"
#include "TailModel.h"

TailModel::TailModel(
  QString const &fqName_, QString const &workerSign_,
  std::shared_ptr<RamenType const> type_,
  std::shared_ptr<EventTime const> eventTime_,
  QObject *parent)
  : QAbstractTableModel(parent),
    eventTime(eventTime_),
    fqName(fqName_),
    workerSign(workerSign_),
    keyPrefix("tails/" + fqName.toStdString() + "/" +
              workerSign.toStdString() + "/lasts/"),
    type(type_)
{
  tuples.reserve(500);

  connect(kvs, &KVStore::keyChanged,
          this, &TailModel::onChange);

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

void TailModel::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
        addTuple(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

void TailModel::addTuple(std::string const &key, KValue const &kv)
{
  if (! startsWith(key, keyPrefix)) return;

  std::shared_ptr<conf::Tuples const> batch =
    std::dynamic_pointer_cast<conf::Tuples const>(kv.val);

  if (! batch) {
    qCritical() << "Received tuples that are not tuples:" << *kv.val;
    return;
  }

  size_t const numTuples(batch->tuples.size());
  if (0 == numTuples) return;

  beginInsertRows(QModelIndex(), tuples.size(), tuples.size() + numTuples - 1);

  for (conf::Tuples::Tuple const &tuple : batch->tuples) {
    std::shared_ptr<RamenValue const> val(tuple.unserialize(type));
    if (! val) {
      qCritical() << "Cannot unserialize tuple from batch";
      continue;
    }

    /* If a function has no event time info, all tuples will have time 0.
     * Past data is disabled in that case anyway. */
    double start(eventTime ?
      eventTime->startOfTuple(*val).value_or(0.) : 0.);

    minEventTime_ =
      std::isnan(minEventTime_) ? start : std::min(minEventTime_, start);
    maxEventTime_ =
      std::isnan(maxEventTime_) ? start : std::max(maxEventTime_, start);
    order.insert(std::make_pair(start, tuples.size()));
    tuples.emplace_back(start, val);
  }
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
  return type->vtyp->numColumns();
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
        QVariant(tuples[row].second->columnValue(column)->toQString(std::string()));
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
      return type->vtyp->columnName(section);
    case Qt::Vertical:
      if (section < 0 || section >= rowCount()) return QVariant();
      return QVariant(QString::number(section));
  }
  return QVariant();
}

bool TailModel::isNumeric(int column) const
{
  return type->vtyp->columnType(column)->vtyp->isNumeric();
}
