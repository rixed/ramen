#ifndef STORAGETREEMODEL_H_190930
#define STORAGETREEMODEL_H_190930
#include <QSortFilterProxyModel>

/* Restriction of a GraphModel keeping only the functions with archives.
 *
 * And if you wonder why this class is not inline despite how small it is,
 * it's because of Qt MOC preprocessor want things that way. */

class StorageTreeModel : public QSortFilterProxyModel
{
  Q_OBJECT

public:
  StorageTreeModel(QObject *parent = nullptr);

protected:
  bool filterAcceptsRow(int, const QModelIndex &) const override;
};

#endif
