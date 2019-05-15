#ifndef TAILMODEL_H_190515
#define TAILMODEL_H_190515
#include <QAbstractItemModel>

/* The model representing lines of tuples, with possibly some tuples skipped
 * in between 2 lines. The model stores *all* tuples of *all* tables for which
 * we receive tuples, regardless of whether there is a widget currently
 * displaying it, and regardless of whether we actually have asked for these
 * tuples (note: actually, tuples themselves are stored in the FunctionItems).
 * This way, we can simply transfer received values to this model which will
 * (write only) which will also act as a cache.
 *
 * On top of this giant map from tables to set of tuples, we want the table
 * abstraction with rows and columns. This is made easier by the fact that
 * we never delete anything, so rows and columns are forever and we merely
 * assign them their index at creation time.
 *
 * Note regarding indexing:
 * We will have a tree of depth 2: at the root, one row (of 1 column) for each
 * worker, and then as many rows and columns as required to store the tuples.
 *
 * Notice that this model is not the one needed to display a table of tuples.
 * For this one need to instantiate a SubModel with a given FunctionItem,
 * which will be a flat model for only that function.
 */

class FunctionItem;

class TailModel : public QAbstractItemModel
{
  Q_OBJECT

  friend class TailSubModel;

  // Those FunctionItem are still owned by the GraphModel:
  std::vector<FunctionItem const *> functions;

  QVariant functionData(FunctionItem const *, int role) const;
  QVariant tupleData(FunctionItem const *, int row, int column, int role) const;

public:
  TailModel();

  QModelIndex index(int row, int column, QModelIndex const &parent) const;
  QModelIndex parent(QModelIndex const &index) const;
  int rowCount(QModelIndex const &parent) const;
  int columnCount(QModelIndex const &parent) const;
  QVariant data(QModelIndex const &index, int role) const;

public slots:
  void addFunction(FunctionItem const *);
};

#endif
