#include "TailModel.h"
#include "FunctionItem.h"
#include "TailSubModel.h"

TailSubModel::TailSubModel(TailModel *model_, FunctionItem const *f_) :
  model(model_), f(f_)
{
}

QModelIndex TailSubModel::index(int row, int column, QModelIndex const &parent) const
{
  if (!parent.isValid()) return createIndex(row, column, nullptr);
  return QModelIndex();
}

QModelIndex TailSubModel::parent(QModelIndex const &) const
{
  return QModelIndex();
}

int TailSubModel::rowCount(QModelIndex const &) const
{
  return f->tuples.size();
}

int TailSubModel::columnCount(QModelIndex const &) const
{
  return f->numColumns();
}

QVariant TailSubModel::data(QModelIndex const &index, int role) const
{
  QModelIndex i = model->createIndex(index.row(), index.column(), (void *)f);
  return model->data(i, role);
}
