#ifndef SOURCESWINWIN_H_20190429
#define SOURCESWINWIN_H_20190429
#include <string>
#include "SavedWindow.h"
/* The SourcesWindow is the initial window that's opened.
 * It features the SourcesView, aka the code editor.
 * Error messages will also appear in this window status bar. */

#define SOURCE_EDITOR_WINDOW_NAME "EditorWindow"

class QWidget;
class ConfTreeModel;
class SourcesModel;
class GraphModel;

class SourcesWin : public SavedWindow
{
  Q_OBJECT

  ConfTreeModel *confTreeModel;
  SourcesModel *sourcesModel;

public:
  explicit SourcesWin(QWidget *parent = nullptr);
};

#endif
