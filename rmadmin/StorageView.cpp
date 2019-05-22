#include <QVBoxLayout>
#include "StorageInfo.h"
#include "StorageTableView.h"
#include "StoragePies.h"
#include "StorageTimeline.h"
#include "StorageView.h"

StorageView::StorageView(GraphModel *graphModel, QWidget *parent) :
  QWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;

  // First some text, with some raw numbers and the edit form:
  StorageInfo *info = new StorageInfo(graphModel, this);
  layout->addWidget(info);

  // Then a treeview of workers, with special color for those which are currently
  // archiving, with columns for tot number of archive files, bytes, and a timeline.
  StorageTableView *tblView = new StorageTableView(this);
  layout->addWidget(tblView);

  // Then some pie charts
  StoragePies *pies = new StoragePies(this);
  layout->addWidget(pies);

  // Then a timeline for the selected worker
  // with a graph of the node + its parents
  StorageTimeline *time = new StorageTimeline(this);
  layout->addWidget(time);

  setLayout(layout);
}
