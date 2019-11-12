#include <QTabWidget>
#include "SourcesModel.h"
#include "SourcesView.h"
#include "SourcesWin.h"

SourcesWin::SourcesWin(QWidget *parent) :
  SavedWindow(SOURCE_EDITOR_WINDOW_NAME, tr("Code Editor"), true, parent)
{
  sourcesModel = new SourcesModel(this);
  sourcesView = new SourcesView(sourcesModel, this);
  setCentralWidget(sourcesView);
}

void SourcesWin::showFile(std::string const &keyPrefix)
{
  sourcesView->showFile(keyPrefix);
}
