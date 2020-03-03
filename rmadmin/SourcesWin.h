#ifndef SOURCESWINWIN_H_20190429
#define SOURCESWINWIN_H_20190429
#include <string>
#include "SavedWindow.h"
/* The SourcesWindow is the initial window that's opened.
 * It features the SourcesView, aka the code editor.
 * Error messages will also appear in this window status bar. */

#define SOURCE_EDITOR_WINDOW_NAME "EditorWindow"

class ConfTreeModel;
class QWidget;
class SourcesModel;
class SourcesView;

class SourcesWin : public SavedWindow
{
  Q_OBJECT

  SourcesModel *sourcesModel;
  SourcesView *sourcesView;

public:
  explicit SourcesWin(QWidget *parent = nullptr);

public slots:
  /* The string is the key prefix of the desired source */
  void showFile(std::string const &);
};

#endif
