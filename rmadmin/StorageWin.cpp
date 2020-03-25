#include <QLabel>
#include "StorageView.h"
#include "GraphModel.h"
#include "StorageWin.h"

StorageWin::StorageWin(QWidget *parent) :
  SavedWindow("Storage", tr("Storage"), true, parent)
{
  if (GraphModel::globalGraphModel) {
    // TODO: a globalGraphModelWithoutTopHalves
    StorageView *storageView = new StorageView(GraphModel::globalGraphModel);
    setCentralWidget(storageView);
  } else {
    QString errMsg(tr("No graph model yet!?"));
    setCentralWidget(new QLabel(errMsg));
    // Better luck next time?
    setAttribute(Qt::WA_DeleteOnClose);
  }
}
