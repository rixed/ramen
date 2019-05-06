#include <iostream>
#include "StorageForm.h"
#include "RmAdminWin.h"

RmAdminWin::RmAdminWin(QWidget *parent) :
    QMainWindow(parent)
{
  StorageForm *w = new StorageForm();
  setCentralWidget(w);
  setWindowTitle(tr("RmAdmin"));

  errorMessage = new KErrorMsg(conf::my_errors);
  statusBar()->addPermanentWidget(errorMessage);

  /* Must not wait that the connProgress slot create the statusBar, as
   * it will be called from another thread: */
  statusBar()->showMessage(tr("Starting-up..."));
}

RmAdminWin::~RmAdminWin()
{
  delete errorMessage;
}

void RmAdminWin::setStatusMsg()
{
  QString msg =
    tr("Connection: ").
    append(connStatus.message()).
    append(tr(", Authentication: ")).
    append(authStatus.message()).
    append(tr(", Synchronization: ")).
    append(syncStatus.message());
  statusBar()->showMessage(msg);
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
