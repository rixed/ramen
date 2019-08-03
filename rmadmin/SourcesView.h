#ifndef SOURCESVIEW_H_190530
#define SOURCESVIEW_H_190530
#include <QSplitter>
#include "conf.h"

class QTreeView;
class SourcesModel;
class CodeEdit;
class QLabel;

class SourcesView : public QSplitter
{
  Q_OBJECT

  QTreeView *sourcesList;
  CodeEdit *editor;
  QLabel *noSelection;

  SourcesModel *sourcesModel;

public:
  SourcesView(SourcesModel *, QWidget *parent = nullptr);

public slots:
  // Request that this program be Shown/focused in the code editor:
  void showFile(conf::Key const &);
  // Popup that displays the full content of the info of that source:
  void openInfo(QModelIndex const &);
  // Create a new program with that source
  void runSource(QModelIndex const &);
};

#endif
