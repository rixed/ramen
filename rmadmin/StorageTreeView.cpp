#include <QTimer>
#include "misc.h"
#include "StorageTreeModel.h"
#include "GraphModel.h"
#include "FunctionItem.h"
#include "StorageTreeView.h"

StorageTreeView::StorageTreeView(GraphModel *graphModel, QWidget *parent) :
  QTreeView(parent)
{
  invalidateModelTimer = new QTimer(this);
  invalidateModelTimer->setSingleShot(true);

  storageTreeModel = new StorageTreeModel(this);
  storageTreeModel->setSourceModel(graphModel);
  setModel(storageTreeModel);

  // Display only the columns that are relevant to archival:
  for (unsigned c = 0; c < GraphModel::NumColumns; c ++) {
    if (!GraphModel::columnIsAboutArchives((GraphModel::Columns)c)) {
      setColumnHidden(c, true);
    }
  }
  connect(graphModel, &GraphModel::storagePropertyChanged,
          this, &StorageTreeView::mayInvalidateModel);
  connect(invalidateModelTimer, &QTimer::timeout,
          this, &StorageTreeView::doInvalidateModel);

  // Cosmetics:
  setAlternatingRowColors(true);
  setSortingEnabled(true);
  sortByColumn(0, Qt::AscendingOrder);

  // Make sure all new rows are always initially expanded:
  expandAll();
  connect(storageTreeModel, &QAbstractItemModel::rowsInserted,
          this, &StorageTreeView::expandRows);
}

// FIXME: Does not work for some reason:
void StorageTreeView::expandRows(QModelIndex const &parent, int first, int last)
{
  expandAllFromParent(this, parent, first, last);
}

void StorageTreeView::mayInvalidateModel()
{
  static int const invalidateModelTimeout = 1000; // ms
  invalidateModelTimer->start(invalidateModelTimeout);
}

void StorageTreeView::doInvalidateModel()
{
  storageTreeModel->invalidate();
}
