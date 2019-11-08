#include <QTabWidget>
#include "SourcesModel.h"
#include "SourcesView.h"
#include "OperationsView.h"
#include "GraphModel.h"
#include "StorageView.h"
#include "SourcesWin.h"

SourcesWin::SourcesWin(QWidget *parent) :
  SavedWindow(SOURCE_EDITOR_WINDOW_NAME, tr("Code Editor"), true, parent)
{
  bool const with_beta_features = getenv("RMADMIN_BETA");

  sourcesModel = new SourcesModel(this);
  if (with_beta_features) {
    // For now have a tabbar with the available views:
    QTabWidget *tw = new QTabWidget(this);

    tw->addTab(new SourcesView(sourcesModel), tr("&Sources"));
    tw->addTab(new OperationsView(GraphModel::globalGraphModel), tr("&Operations"));
    tw->addTab(new StorageView(GraphModel::globalGraphModel), tr("&Storage"));

    tw->setCurrentIndex(0); // DEBUG

    setCentralWidget(tw);
  } else {
    setCentralWidget(new SourcesView(sourcesModel));
  }
}
