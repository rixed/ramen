#include <QGridLayout>
#include "GraphModel.h"
#include "StorageInfo.h"
#include "StorageTreeView.h"
#include "StoragePies.h"
#include "StorageTimeline.h"
#include "StorageTreeModel.h"
#include "StorageView.h"

StorageView::StorageView(GraphModel *graphModel, QWidget *parent) :
  QWidget(parent)
{
  QGridLayout *layout = new QGridLayout;

  // First some text, with some raw numbers and the edit form:
  StorageInfo *info = new StorageInfo(graphModel, this);
  layout->addWidget(info, 0, 0);

  StorageTreeModel *storageTreeModel = new StorageTreeModel(this);
  storageTreeModel->setSourceModel(graphModel);

  // Then a table of archiving workers, with columns for tot number of
  // archive files, bytes, and a timeline.
  StorageTreeView *tblView =
    new StorageTreeView(graphModel, storageTreeModel, this);
  layout->addWidget(tblView, 1, 0);

  // Then some pie charts
  StoragePies *pies = new StoragePies(graphModel, this);
  layout->addWidget(pies, 0, 1, 2, 1);

  // Then a timeline for the selected worker
  // with a graph of the node + its parents
  StorageTimeline *time =
    new StorageTimeline(graphModel, this);
  layout->addWidget(time, 2, 0, 1, 2);

  setLayout(layout);
}
