#include <cassert>
#include <string>
#include <memory>
#include "FunctionItem.h"
#include "conf.h"
#include "confKey.h"
#include "confValue.h"
#include "TailModel.h"

static std::string user_id("admin"); // TODO

static conf::Key tailKey(FunctionItem const *f)
{
  return conf::Key("tail/" + f->fqName().toStdString() + "/users/" + user_id);
}

TailModel::TailModel(FunctionItem const *f_) :
  f(f_)
{
  // Propagates this function's signals into our beginInsertRows
  QObject::connect(f, &FunctionItem::beginAddTuple, this, [this](int first, int last) {
    // Convert the signal by adding the QModelIndex for that function:
    emit beginInsertRows(QModelIndex(), first, last);
  });
  QObject::connect(f, &FunctionItem::endAddTuple, this, &TailModel::endInsertRows);

  // Subscribe to that table tail:
  conf::Key k = tailKey(f);
  // TODO: have a VoidType
  std::shared_ptr<conf::Value> v = std::shared_ptr<conf::Value>(new conf::Bool(true));
  conf::askNew(k, v);
}

TailModel::~TailModel()
{
  conf::askDel(tailKey(f));
}

/* Given we only need a flat model (table not tree) we only use an invalid index as
 * parent and points to this for internal pointer. If we are given a valid parent
 * we must return an invalid index to signal the end of the tree. */
QModelIndex TailModel::index(int row, int column, QModelIndex const &parent) const
{
  if (parent.isValid()) return QModelIndex();

  if (row >= (int)f->tuples.size() || column >= (int)f->numColumns())
    return QModelIndex();

  return createIndex(row, column, (void *)this);
}

QModelIndex TailModel::parent(QModelIndex const &) const
{
  return QModelIndex();
}

int TailModel::rowCount(QModelIndex const &parent) const
{
  if (parent.isValid()) return 0;
  return f->tuples.size();
}

int TailModel::columnCount(QModelIndex const &parent) const
{
  if (parent.isValid()) return 0;
  return f->numColumns();
}

QVariant TailModel::data(QModelIndex const &index, int role) const
{
  if (!index.isValid()) return QVariant();

  int const row = index.row();
  int const column = index.column();

  if (row >= (int)f->tuples.size() || column >= (int)f->numColumns())
    return QVariant();

  switch (role) {
    case Qt::DisplayRole:
      // TODO
      return QVariant(QString::number(row) + ", " + QString::number(column));
    case Qt::ToolTipRole:
      // TODO
      return QVariant(QString("Column #") + QString::number(column));
    case Qt::WhatsThisRole:
      // TODO
      return QVariant(QString(tr("What's \"What's this\"?")));
    default:
      return QVariant();
  }
}

QVariant TailModel::headerData(int section, Qt::Orientation orient, int role) const
{
  if (role != Qt::DisplayRole) return QVariant();
  switch (orient) {
    case Qt::Horizontal:
      if (section >= f->numColumns()) return QVariant();
      f->header(section);
      break;
    case Qt::Vertical:
      if (section >= f->numRows()) return QVariant();
      return QVariant(QString::number(section));
  }
  return QVariant();
}
