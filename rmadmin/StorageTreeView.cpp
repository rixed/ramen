#include "misc.h"
#include "StorageTreeModel.h"
#include "GraphModel.h"
#include "FunctionItem.h"
#include "StorageTreeView.h"

StorageTreeView::StorageTreeView(GraphModel *graphModel, QWidget *parent) :
  QTreeView(parent)
{
  StorageTreeModel *model = new StorageTreeModel(this);
  model->setSourceModel(graphModel);
  setModel(model);

  // Display only the columns that are relevant to archiving:
  for (unsigned c = 0; c < GraphModel::NumColumns; c ++) {
    if (!GraphModel::columnIsAboutArchives((GraphModel::Columns)c)) {
      setColumnHidden(c, true);
    }
  }
  connect(graphModel, &GraphModel::storagePropertyChanged,
          model, &QSortFilterProxyModel::invalidate);

  // Cosmetics:
  setAlternatingRowColors(true);
  setSortingEnabled(true);
  sortByColumn(0, Qt::AscendingOrder);

  // Make sure all new rows are always initially expanded:
  expandAll();
  connect(model, &QAbstractItemModel::rowsInserted,
          this, &StorageTreeView::expandRows);
}

// FIXME: Does not work for some reason:
void StorageTreeView::expandRows(QModelIndex const &parent, int first, int last)
{
  expandAllFromParent(this, parent, first, last);
}
