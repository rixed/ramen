#ifndef TAILMODEL_H_190515
#define TAILMODEL_H_190515
#include <QAbstractItemModel>

/* The model representing lines of tuples, with possibly some tuples skipped
 * in between 2 lines. The model stores *all* tuples of *all* tables for which
 * we receive tuples, regardless of whether there is a widget currently
 * displaying it, and regardless of whether we actually have asked for these
 * tuples (actually, tuples themselves are stored in the FunctionItems. Shall
 * tuples be received for which no FunctionItem is known, they might simply be
 * ignored).
 *
 * On top of this giant map from tables to set of tuples, we want the table
 * abstraction with rows and columns. This is made easier by the fact that
 * we never delete anything, so rows and columns are forever and we merely
 * assign them their index at creation time.
 */

class FunctionItem;

class TailModel : public QAbstractTableModel
{
  Q_OBJECT

  FunctionItem const *f;
  bool used;  // TODO: after a while, functions with unused model destroy them

public:
  TailModel(FunctionItem const *, QObject *parent = nullptr);
  ~TailModel();
  int rowCount(QModelIndex const &parent) const override;
  int columnCount(QModelIndex const &parent) const override;
  QVariant data(QModelIndex const &index, int role) const override;
  QVariant headerData(int, Qt::Orientation, int role = Qt::DisplayRole) const override;
  void setUsed(bool used_) { used = used_; }

signals:
  void beginInsertRows(QModelIndex const &, int, int);
  void endInsertRows();
};

#endif
