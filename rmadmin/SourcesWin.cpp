#include <iostream>
#include <QTabWidget>
#include "KErrorMsg.h"
#include "SourcesModel.h"
#include "SourcesView.h"
#include "OperationsView.h"
#include "GraphModel.h"
#include "StorageView.h"
#include "SourcesWin.h"

SourcesWin::SourcesWin(QWidget *parent) :
  SavedWindow(SOURCE_EDITOR_WINDOW_NAME, tr("Code Editor"), parent)
{
  bool const with_beta_features = getenv("RMADMIN_BETA");

  sourcesModel = new SourcesModel(this);
  if (with_beta_features) {
    // For now have a tabbar with the available views:
    QTabWidget *tw = new QTabWidget(this);

    tw->addTab(new SourcesView(sourcesModel), tr("&Sources"));
    tw->addTab(new OperationsView(GraphModel::globalGraphModel), tr("&Operations"));
    tw->addTab(new StorageView(GraphModel::globalGraphModel), tr("&Storage"));

    tw->setCurrentIndex(0); // DEBUG

    setCentralWidget(tw);
  } else {
    setCentralWidget(new SourcesView(sourcesModel));
  }

  errorMessage = new KErrorMsg(this);
  statusBar()->addPermanentWidget(errorMessage);

  /* Must not wait that the connProgress slot create the statusBar, as
   * it will be called from another thread: */
  statusBar()->showMessage(tr("Starting-up..."));
}

void SourcesWin::setStatusMsg()
{
  QStatusBar *sb = statusBar(); // create it if it doesn't exist yet
  if (connStatus.isError() ||
      authStatus.isError() ||
      syncStatus.isError())
  {
    sb->setStyleSheet("background-color: pink;");
  }
  QString msg =
    tr("Connection: ").
    append(connStatus.message()).
    append(tr(", Authentication: ")).
    append(authStatus.message()).
    append(tr(", Synchronization: ")).
    append(syncStatus.message());
  sb->showMessage(msg);
}

void SourcesWin::connProgress(SyncStatus status)
{
  connStatus = status;
  setStatusMsg();
}

void SourcesWin::authProgress(SyncStatus status)
{
  authStatus = status;
  setStatusMsg();
}

void SourcesWin::syncProgress(SyncStatus status)
{
  syncStatus = status;
  setStatusMsg();
}
