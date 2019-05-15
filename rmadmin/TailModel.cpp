#include <cassert>
#include "TailModel.h"
#include "FunctionItem.h"

TailModel::TailModel()
{
  functions.reserve(20);
}

/* We use as index thw row/column and the internalPointer just to point
 * to root (null), in which case row gives us the function index, or to
 * a FunctionItem.
 * InternalPointer does not store Tuple address as they are owned by a
 * resizeable vector (FunctionItems, on another hand, does not move -
 * ProgramItem::functions being a vector of FunctionItem*). */
QModelIndex TailModel::index(int row, int column, QModelIndex const &parent) const
{
  if (!parent.isValid() || !parent.internalPointer()) {
    if ((size_t)row >= functions.size() ||
        column > 0)
      return QModelIndex();

    return createIndex(row, column, nullptr);
  }

  // Otherwise parent is a functionItem:
  FunctionItem const *f = static_cast<FunctionItem const *>(parent.internalPointer());
  if ((ssize_t)row >= (ssize_t)f->tuples.size() ||
      column >= (int)f->numColumns())
    return QModelIndex();

  return createIndex(row, column, (void *)f);
}

QModelIndex TailModel::parent(QModelIndex const &index) const
{
  if (!index.isValid()) return QModelIndex();  // Should not happen IMHO

  if (!index.internalPointer()) return QModelIndex();

  FunctionItem const *f = static_cast<FunctionItem const *>(index.internalPointer());
  for (size_t row = 0; row < functions.size(); row++) {
    if (functions[row] == f)
      return createIndex(row, 0, nullptr);
  }

  return QModelIndex();
}

int TailModel::rowCount(QModelIndex const &parent) const
{
  if (!parent.isValid()) return 0;  // Should not happen IMHO

  if (!parent.internalPointer()) return functions.size();

  FunctionItem const *f = static_cast<FunctionItem const *>(parent.internalPointer());
  return f->tuples.size();
}

int TailModel::columnCount(QModelIndex const &parent) const
{
  if (!parent.isValid()) return 0;  // Should not happen IMHO

  if (!parent.internalPointer()) return 1;

  FunctionItem const *f = static_cast<FunctionItem const *>(parent.internalPointer());
  return f->numColumns();
}

QVariant TailModel::data(QModelIndex const &index, int role) const
{
  if (!index.isValid()) return QVariant();

  int const row = index.row();
  if (!index.internalPointer()) {
    if ((ssize_t)row >= (ssize_t)functions.size() ||
        index.column() > 0)
      return QVariant();

    return functionData(functions[row], role);
  } else {
    FunctionItem const *f = static_cast<FunctionItem const *>(index.internalPointer());
    return tupleData(f, row, index.column(), role);
  }
}

QVariant TailModel::functionData(FunctionItem const *f, int role) const
{
  switch (role) {
    case Qt::DisplayRole:
      return QVariant(f->fqName());
    case Qt::ToolTipRole:
      return QVariant(QString(tr("Function name")));
    case Qt::WhatsThisRole:
      return QVariant(QString(tr("What's \"What's this\"?")));
    default:
      return QVariant();
  }
}

QVariant TailModel::tupleData(FunctionItem const *f, int row, int column, int role) const
{
  if ((ssize_t)row >= (ssize_t)f->tuples.size() ||
      column >= (int)f->numColumns())
    return QVariant();

//  Tuple const &tuple = f->tuples[row];

  switch (role) {
    case Qt::DisplayRole:
      return QVariant(QString::number(row) + ", " + QString::number(column));
    default:
      return QVariant();
  }
}

void TailModel::addFunction(FunctionItem const *f)
{
  functions.push_back(f);
}
