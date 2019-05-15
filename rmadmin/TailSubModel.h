#ifndef TAILSUBMODEL_H_190515
#define TAILSUBMODEL_H_190515
#include <QAbstractItemModel>

class TailModel;
class FunctionItem;

class TailSubModel : public QAbstractItemModel
{
  Q_OBJECT

  TailModel *model;
  FunctionItem const *f;
public:
  TailSubModel(TailModel *, FunctionItem const *);
  QModelIndex index(int row, int column, QModelIndex const &parent) const;
  QModelIndex parent(QModelIndex const &index) const;
  int rowCount(QModelIndex const &parent) const;
  int columnCount(QModelIndex const &parent) const;
  QVariant data(QModelIndex const &index, int role) const;
};

#endif
