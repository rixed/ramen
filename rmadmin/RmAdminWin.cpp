#include <iostream>
#include <QTabWidget>
#include "KErrorMsg.h"
#include "ConfTreeWidget.h"
#include "SourcesModel.h"
#include "SourcesView.h"
#include "ProgramsView.h"
#include "OperationsView.h"
#include "GraphModel.h"
#include "StorageView.h"
#include "RmAdminWin.h"

RmAdminWin::RmAdminWin(QWidget *parent) :
    QMainWindow(parent)
{
  sourcesModel = new SourcesModel(this);
  // A GraphModel satisfies both the TreeView and the GraphView
  // requirements:
  settings = new GraphViewSettings;
  graphModel = new GraphModel(settings);

  // For now have a tabbar with the available views:
  QTabWidget *tw = new QTabWidget(this);

  tw->addTab(new ConfTreeWidget, tr("&Raw configuration"));
  tw->addTab(new SourcesView(sourcesModel), tr("&Sources"));
  tw->addTab(new ProgramsView, tr("&Programs"));
  tw->addTab(new OperationsView(graphModel), tr("&Operations"));
  tw->addTab(new StorageView(graphModel), tr("&Storage"));

  tw->setCurrentIndex(0); // DEBUG

  setCentralWidget(tw);
  setWindowTitle(tr("RmAdmin"));

  errorMessage = new KErrorMsg();
  statusBar()->addPermanentWidget(errorMessage);

  /* Must not wait that the connProgress slot create the statusBar, as
   * it will be called from another thread: */
  statusBar()->showMessage(tr("Starting-up..."));
}

RmAdminWin::~RmAdminWin()
{
  delete errorMessage;
  // FIXME: Qt will delete the infoTabs and dataTabs that references objects
  // from the model only *after* we delete the model:
  delete graphModel;
  delete settings;
}

void RmAdminWin::setStatusMsg()
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

void RmAdminWin::connProgress(SyncStatus status)
{
  connStatus = status;
  setStatusMsg();
}

void RmAdminWin::authProgress(SyncStatus status)
{
  authStatus = status;
  setStatusMsg();
}

void RmAdminWin::syncProgress(SyncStatus status)
{
  syncStatus = status;
  setStatusMsg();
}
