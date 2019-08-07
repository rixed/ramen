#ifndef RMADMINWIN_H_20190429
#define RMADMINWIN_H_20190429

#include <string>
#include <QWidget>
#include <QMainWindow>
#include "SyncStatus.h"
#include "confKey.h"
#include "KErrorMsg.h"

class ConfTreeModel;
class SourcesModel;
class GraphModel;
class GraphViewSettings;

class RmAdminWin : public QMainWindow
{
  Q_OBJECT

  SyncStatus connStatus, authStatus, syncStatus;
  void setStatusMsg();
  KErrorMsg *errorMessage;
  ConfTreeModel *confTreeModel;
  SourcesModel *sourcesModel;

public:
  explicit RmAdminWin(GraphModel *, bool with_beta_features, QWidget *parent = nullptr);
  ~RmAdminWin();

signals:

public slots:
  void connProgress(SyncStatus);
  void authProgress(SyncStatus);
  void syncProgress(SyncStatus);
  void setErrorKey(conf::Key const key) { errorMessage->setKey(key); }
};

#endif
