#include <QSplitter>
#include <QTreeView>
#include <QHeaderView>
#include <QTabWidget>
#include "conf.h"
#include "CodeEdit.h"
#include "widgetTools.h"
#include "ButtonDelegate.h"
#include "SourceInfoViewer.h"
#include "NewProgramDialog.h"
#include "ConfTreeEditorDialog.h"
#include "SourcesModel.h"
#include "SourcesView.h"

SourcesView::SourcesView(SourcesModel *sourceModel_, QWidget *parent) :
  QSplitter(parent), sourcesModel(sourceModel_)
{
  sourcesList = new QTreeView(this);
  sourcesList->setModel(sourcesModel);
  sourcesList->setHeaderHidden(true);
  sourcesList->setUniformRowHeights(true);
  sourcesList->setMouseTracking(true);  // for the buttons to follow the mouse
  sourcesList->header()->setStretchLastSection(false);
  sourcesList->header()->setSectionResizeMode(0, QHeaderView::Stretch);
  sourcesList->header()->setSectionResizeMode(1, QHeaderView::ResizeToContents);
  sourcesList->header()->setSectionResizeMode(2, QHeaderView::ResizeToContents);
  /* Note: delegates are not owned by the QTreeView, so let's make this the
   * owner: */
  ButtonDelegate *detailButton = new ButtonDelegate(3, this);
  sourcesList->setItemDelegateForColumn(1, detailButton);
  connect(detailButton, &ButtonDelegate::clicked,
          this, &SourcesView::openInfo);
  ButtonDelegate *runButton= new ButtonDelegate(3, this);
  sourcesList->setItemDelegateForColumn(2, runButton);
  connect(runButton, &ButtonDelegate::clicked,
          this, &SourcesView::runSource);

  setStretchFactor(0, 0);

  sourceTabs = new QTabWidget(this);
  sourceTabs->setTabsClosable(true);
  connect(sourceTabs, &QTabWidget::tabCloseRequested,
          this, &SourcesView::closeSource);
  setStretchFactor(1, 1);

  // Connect selection of a program to the display of its code:
  connect(sourcesList, &QAbstractItemView::activated,
          this, [this](QModelIndex const &index) {
    if (! index.isValid()) return;

    SourcesModel::TreeItem const *item =
      static_cast<SourcesModel::TreeItem const *>(index.internalPointer());
    SourcesModel::FileItem const *file =
      dynamic_cast<SourcesModel::FileItem const *>(item);
    if (file) showFile(file->sourceKey);
  });
}

void SourcesView::showFile(conf::Key const &key)
{
  QString const sourceName(sourceNameOfKey(key));
  if (tryFocusTab(sourceTabs, sourceName)) return;
  CodeEdit *editor = new CodeEdit(key);
  sourceTabs->addTab(editor, sourceName);
  focusLastTab(sourceTabs);
}

void SourcesView::closeSource(int idx)
{
  sourceTabs->removeTab(idx);
}

void SourcesView::openInfo(QModelIndex const &index)
{
  conf::Key const infoKey =
    changeSourceKeyExt(sourcesModel->keyOfIndex(index), "info");

  KValue const *kv = nullptr;
  conf::kvs_lock.lock_shared();
  if (conf::kvs.contains(infoKey)) kv = &conf::kvs[infoKey];
  conf::kvs_lock.unlock_shared();

  if (kv) {
    ConfTreeEditorDialog *dialog = new ConfTreeEditorDialog(infoKey, kv);
    dialog->show();
  } else {
    /* Should not happen as the button is only actionable when the info
     * is present (TODO): */
    std::cerr << "No source info for " << infoKey << std::endl;
  }
}

void SourcesView::runSource(QModelIndex const &index)
{
  QString const sourceName =
    sourceNameOfKey(sourcesModel->keyOfIndex(index));
  NewProgramDialog *dialog = new NewProgramDialog(sourceName);
  dialog->show();
}
