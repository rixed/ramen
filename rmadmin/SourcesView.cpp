#include <QSplitter>
#include <QTreeView>
#include <QTabWidget>
#include "conf.h"
#include "CodeEdit.h"
#include "widgetTools.h"
#include "SourcesModel.h"
#include "SourcesView.h"

SourcesView::SourcesView(SourcesModel *sourceModel_, QWidget *parent) :
  QSplitter(parent), sourcesModel(sourceModel_)
{
  sourcesList = new QTreeView(this);
  sourcesList->setModel(sourcesModel);
  sourcesList->setHeaderHidden(true);
  sourcesList->setUniformRowHeights(true);
  setStretchFactor(0, 0);

  sourceTabs = new QTabWidget(this);
  sourceTabs->setTabsClosable(true);
  connect(sourceTabs, &QTabWidget::tabCloseRequested,
          this, &SourcesView::closeSource);
  setStretchFactor(1, 1);

  // Connect selection of a program to showing its code:
  connect(sourcesList, &QAbstractItemView::activated,
          this, [this](QModelIndex const &index) {
    if (! index.isValid()) return;

    SourcesModel::TreeItem const *item =
      static_cast<SourcesModel::TreeItem const *>(index.internalPointer());
    SourcesModel::FileItem const *file =
      dynamic_cast<SourcesModel::FileItem const *>(item);
    if (file) showFile(file->sourceName);
  });
}

void SourcesView::showFile(QString const sourceName)
{
  if (tryFocusTab(sourceTabs, sourceName)) return;
  CodeEdit *editor = new CodeEdit(sourceName);
  sourceTabs->addTab(editor, sourceName);
  focusLastTab(sourceTabs);
}

void SourcesView::closeSource(int idx)
{
  sourceTabs->removeTab(idx);
}
