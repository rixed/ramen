#include <cassert>
#include <string>
#include <memory>
#include "FunctionItem.h"
#include "conf.h"
#include "confKey.h"
#include "confValue.h"
#include "TailModel.h"

static conf::Key tailKey(std::shared_ptr<Function const> f)
{
  return conf::Key("tails/" + f->fqName.toStdString() +
                   "/users/" + my_uid->toStdString());
}

TailModel::TailModel(std::shared_ptr<Function const> f_, QObject *parent) :
  QAbstractTableModel(parent),
  f(f_)
{
  // Propagates this function's signals into our beginInsertRows
  connect(f.get(), &Function::beginAddTuple,
          this, &TailModel::beginInsertRows);
  connect(f.get(), &Function::endAddTuple,
          this, &TailModel::endInsertRows);

  // Subscribe to that table tail:
  conf::Key k = tailKey(f);
  // TODO: have a VoidType
  std::shared_ptr<conf::Value> v(new conf::RamenValueValue(new VBool(true)));
  conf::askSet(k, v);
}

TailModel::~TailModel()
{
  conf::askDel(tailKey(f));
}

int TailModel::rowCount(QModelIndex const &parent) const
{
  if (parent.isValid()) return 0;
  return f->numRows();
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

  if (row < 0 || row >= (int)f->numRows() ||
      column < 0 || column >= (int)f->numColumns())
    return QVariant();

  switch (role) {
    case Qt::DisplayRole:
      {
        RamenValue const *v = f->tupleData(row, column);
        if (! v) return QVariant(QString("No such column"));
        return QVariant(v->toQString(conf::Key::null));
      }
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
  if (role != Qt::DisplayRole)
    return QAbstractTableModel::headerData(section, orient, role);

  switch (orient) {
    case Qt::Horizontal:
      if (section < 0 || section >= f->numColumns()) return QVariant();
      return f->header(section);
      break;
    case Qt::Vertical:
      if (section < 0 || section >= f->numRows()) return QVariant();
      return QVariant(QString::number(section));
  }
  return QVariant();
}
