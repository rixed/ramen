#include <string>
#include <memory>
#include "TailModel.h"
#include "FunctionItem.h"
#include "conf.h"
#include "confKey.h"
#include "confValue.h"
#include "TailSubModel.h"

static std::string user_id("admin");

static conf::Key tailKey(FunctionItem const *f)
{
  return conf::Key("tail/" + f->fqName().toStdString() + "/users/" + user_id);
}

TailSubModel::TailSubModel(TailModel *model_, FunctionItem const *f_) :
  model(model_), f(f_)
{
  // Subscribe to that table tail:
  conf::Key k = tailKey(f);
  // TODO: have a VoidType
  std::shared_ptr<conf::Value> v = std::shared_ptr<conf::Value>(new conf::Bool(true));
  conf::askSet(k, v);
}

TailSubModel::~TailSubModel()
{
  conf::askDel(tailKey(f));
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
