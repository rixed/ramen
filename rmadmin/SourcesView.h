#ifndef SOURCESVIEW_H_190530
#define SOURCESVIEW_H_190530
#include <QSplitter>
#include "conf.h"

class QTreeView;
class QTabWidget;
class SourcesModel;

class SourcesView : public QSplitter
{
  Q_OBJECT

  QTreeView *sourcesList;
  QTabWidget *sourceTabs;

  SourcesModel *sourcesModel;

public:
  SourcesView(SourcesModel *, QWidget *parent = nullptr);

public slots:
  // Request that this program be Shown/focused in the code editor:
  void showFile(conf::Key const &);
  // Close this program from the code editor:
  void closeSource(int idx);
  // Popup that displays the full content of the info of that source:
  void openInfo(QModelIndex const &);
  // Create a new program with that source
  void runSource(QModelIndex const &);
};

#endif
