#ifndef STORAGETREEVIEW_H_190522
#define STORAGETREEVIEW_H_190522
#include <QTreeView>
/* A simple table displaying all archiving workers with stats related to
 * archival */
class GraphModel;
class QTimer;
class StorageTreeModel;

class StorageTreeView : public QTreeView
{
  Q_OBJECT

  QTimer *invalidateModelTimer;

public:
  StorageTreeView(GraphModel *, StorageTreeModel *, QWidget *parent = nullptr);

protected slots:
  void expandRows(const QModelIndex &, int, int);
  void mayInvalidateModel();
  void doInvalidateModel();
};

#endif
