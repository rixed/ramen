#ifndef RMADMINWIN_H_20190429
#define RMADMINWIN_H_20190429

#include <string>
#include <QWidget>
#include <QMainWindow>
#include "KErrorMsg.h"
#include "SyncStatus.h"

class SourcesModel;
class GraphModel;
class GraphViewSettings;

class RmAdminWin : public QMainWindow
{
  Q_OBJECT

  SyncStatus connStatus, authStatus, syncStatus;
  void setStatusMsg();
  KErrorMsg *errorMessage;
  SourcesModel *sourcesModel;
  GraphModel *graphModel;
  GraphViewSettings *settings;

public:
  explicit RmAdminWin(QWidget *parent = nullptr);
  ~RmAdminWin();

signals:

public slots:
  void connProgress(SyncStatus);
  void authProgress(SyncStatus);
  void syncProgress(SyncStatus);
};

#endif
