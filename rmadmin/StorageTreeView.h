#ifndef STORAGETREEVIEW_H_190522
#define STORAGETREEVIEW_H_190522
#include <QTreeView>

class QTimer;
class GraphModel;
class StorageTreeModel;

class StorageTreeView : public QTreeView
{
  Q_OBJECT

  QTimer *invalidateModelTimer;
  StorageTreeModel *storageTreeModel;

public:
  StorageTreeView(GraphModel *graphModel, QWidget *parent = nullptr);

protected slots:
  void expandRows(const QModelIndex &, int, int);
  void mayInvalidateModel();
  void doInvalidateModel();
};

#endif
