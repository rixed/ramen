#ifndef SOURCESWINWIN_H_20190429
#define SOURCESWINWIN_H_20190429
#include <string>
#include <QWidget>
#include "SavedWindow.h"
#include "SyncStatus.h"
#include "KErrorMsg.h"
/* The SourcesWindow is the initial window that's opened.
 * It features the SourcesView, aka the code editor.
 * Error messages will also appear in this window status bar. */

#define SOURCE_EDITOR_WINDOW_NAME "EditorWindow"

class ConfTreeModel;
class SourcesModel;
class GraphModel;

class SourcesWin : public SavedWindow
{
  Q_OBJECT

  SyncStatus connStatus, authStatus, syncStatus;
  void setStatusMsg();
  KErrorMsg *errorMessage;
  ConfTreeModel *confTreeModel;
  SourcesModel *sourcesModel;

public:
  explicit SourcesWin(QWidget *parent = nullptr);

public slots:
  void connProgress(SyncStatus);
  void authProgress(SyncStatus);
  void syncProgress(SyncStatus);
  void setErrorKey(std::string const key) { errorMessage->setKey(key); }
};

#endif
