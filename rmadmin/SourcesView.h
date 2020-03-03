#ifndef SOURCESVIEW_H_190530
#define SOURCESVIEW_H_190530
#include <QSplitter>
#include <QTreeView>
#include "conf.h"
/* The SourcesView displays on the left the list of all known sources
 * and on the right an editor window. */
class CodeEditForm;
class GraphItem;
class QLabel;
class QStackedLayout;
class SourcesModel;

/* Subclassing the QTreeView is necessary in order to customize the
 * keyboard handling to make it possible to select an entry with a key.
 * Note: have to be defined in a .h for moc to find it: */
class SourcesTreeView : public QTreeView
{
  Q_OBJECT

public:
  SourcesTreeView(QWidget *parent = nullptr);

protected:
  void keyPressEvent(QKeyEvent *);
};


class SourcesView : public QSplitter
{
  Q_OBJECT

  SourcesTreeView *sourcesList;
  /* The editor for any type of sources: */
  CodeEditForm *editorForm;
  /* A message when no source files is selected: */
  QLabel *noSelection;
  /* On the right of the splitter is shown either one of the above: */
  QStackedLayout *rightLayout;
  int codeEditorIndex;
  int noSelectionIndex;

  SourcesModel *sourcesModel;

public:
  SourcesView(SourcesModel *, QWidget *parent = nullptr);

public slots:
  /* Request that that index is shown in the file editor, if it is a file
   * (uses showFile): */
  void showIndex(QModelIndex const &);
  /* Request that the source for this key prefix be shown/focused in the proper
   * code editor (depending on the last modified available extension): */
  void showFile(std::string const &);
  // Request that no file is shown in the code editor:
  void hideFile();
  // Popup that displays the full content of the info of that source:
  void openInfo(QModelIndex const &);
  // Create a new program with that source
  void runSource(QModelIndex const &);
  // Expand that item recursively
  void expandRows(QModelIndex const &parent, int first, int last);
  // Close the editor if it's currently displaying anything in that range
  void hideEditor(QModelIndex const &parent, int first, int last);
};

#endif
