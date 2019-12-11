#include <QDebug>
#include <QHeaderView>
#include <QTimer>
#include "FunctionItem.h"
#include "GraphModel.h"
#include "misc.h"
#include "StorageTreeModel.h"
#include "StorageTreeView.h"

StorageTreeView::StorageTreeView(
    GraphModel *graphModel,
    StorageTreeModel *storageTreeModel,
    QWidget *parent)
  : QTreeView(parent)
{
  setModel(storageTreeModel);

  setUniformRowHeights(true);

  invalidateModelTimer = new QTimer(this);
  invalidateModelTimer->setSingleShot(true);

  // Display only the columns that are relevant to archival:
  for (unsigned c = 0; c < GraphModel::NumColumns; c ++) {
    if (!GraphModel::columnIsAboutArchives((GraphModel::Columns)c)) {
      setColumnHidden(c, true);
    }
  }

  header()->setSectionResizeMode(QHeaderView::ResizeToContents);

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
  StorageTreeModel *storageTreeModel =
    dynamic_cast<StorageTreeModel *>(model());
  if (! storageTreeModel) {
    qCritical() << "Cannot invalidate model: not a StorageTreeModel!?";
    return;
  }

  storageTreeModel->invalidate();
}
