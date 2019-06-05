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
  // Request that this program be Shown/focused
  void showFile(QString const sourceName);
  void closeSource(int idx);
};

#endif
