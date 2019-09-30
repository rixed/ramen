#ifndef STORAGETREEVIEW_H_190522
#define STORAGETREEVIEW_H_190522
#include <QTreeView>

class GraphModel;

class StorageTreeView : public QTreeView
{
  Q_OBJECT
public:
  StorageTreeView(GraphModel *graphModel, QWidget *parent = nullptr);

protected slots:
  void expandRows(const QModelIndex &, int, int);
};

#endif
