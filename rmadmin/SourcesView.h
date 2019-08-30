#ifndef SOURCESVIEW_H_190530
#define SOURCESVIEW_H_190530
#include <QSplitter>
#include "conf.h"

class MyTreeView;
class SourcesModel;
class CodeEdit;
class QLabel;
class QStackedLayout;

class SourcesView : public QSplitter
{
  Q_OBJECT

  MyTreeView *sourcesList;
  CodeEdit *editor;
  QLabel *noSelection;
  QStackedLayout *rightLayout;
  int editorIndex;
  int noSelectionIndex;

  SourcesModel *sourcesModel;

public:
  SourcesView(SourcesModel *, QWidget *parent = nullptr);

public slots:
  /* Request that that index is shown in the file editor, it it is a file
   * (uses showFile): */
  void showIndex(QModelIndex const &);
  // Request that this program be Shown/focused in the code editor:
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
