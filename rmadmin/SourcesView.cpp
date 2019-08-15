#include <iostream>
#include <QSplitter>
#include <QTreeView>
#include <QKeyEvent>
#include <QHeaderView>
#include <QLabel>
#include <QStackedLayout>
#include <QVBoxLayout>
#include "conf.h"
#include "CodeEdit.h"
#include "AtomicForm.h"
#include "widgetTools.h"
#include "ButtonDelegate.h"
#include "SourceInfoViewer.h"
#include "NewProgramDialog.h"
#include "ConfTreeEditorDialog.h"
#include "SourcesModel.h"
#include "SourcesView.h"

static const bool verbose = true;

/* Subclassing the QTreeView is necessary in order to customize the
 * keyboard handling to make it possible to select an entry with a key. */
class MyTreeView : public QTreeView
{
public:
  MyTreeView(QWidget *parent = nullptr) : QTreeView(parent)
  {
  }

protected:
  void keyPressEvent(QKeyEvent *event)
  {
    QTreeView::keyPressEvent(event);

    switch (event->key()) {
      case Qt::Key_Space:
      case Qt::Key_Select:
      case Qt::Key_Enter:
      case Qt::Key_Return:
        QModelIndex const index = currentIndex();
        if (index.isValid()) {
          emit QTreeView::activated(index);
        }
    }
  }
};

SourcesView::SourcesView(SourcesModel *sourceModel_, QWidget *parent) :
  QSplitter(parent), sourcesModel(sourceModel_)
{
  sourcesList = new MyTreeView(this);
  sourcesList->setModel(sourcesModel);
  sourcesList->setHeaderHidden(true);
  sourcesList->setUniformRowHeights(true);
  sourcesList->setMouseTracking(true);  // for the buttons to follow the mouse
  sourcesList->header()->setStretchLastSection(false);
  sourcesList->header()->setSectionResizeMode(0, QHeaderView::Stretch);
  for (int c = 1; c <= 2; c ++) {
    sourcesList->header()->setSectionResizeMode(c, QHeaderView::Fixed);
    sourcesList->header()->setDefaultSectionSize(30);
  }

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

  addWidget(sourcesList);
  setStretchFactor(0, 0);

  rightLayout = new QStackedLayout;

  editor = new CodeEdit;
  editorIndex = rightLayout->addWidget(editor);

  noSelection =
    new QLabel(tr("Select a source file on the left to view/edit it."));
  noSelection->setWordWrap(true);
  noSelection->setAlignment(Qt::AlignCenter);
  noSelectionIndex = rightLayout->addWidget(noSelection);
  rightLayout->setCurrentIndex(noSelectionIndex);

  QWidget *rightPanel = new QWidget;
  rightPanel->setLayout(rightLayout);
  addWidget(rightPanel);
  setStretchFactor(1, 1);

  // Connect selection of a program to the display of its code:
  connect(sourcesList, &MyTreeView::activated,
          this, &SourcesView::showIndex);
  connect(sourcesList, &MyTreeView::clicked,
          this, &SourcesView::showIndex);

  /* Connect the edition start/stop of the code to disabling/reenabling selection
   * in the QTreeWidget: */
  connect(editor->editorForm, &AtomicForm::changeEnabled,
          sourcesList, &MyTreeView::setDisabled);

  /* Connect the deletion of a source to hidding the editor if that's the
   * current source: */
  connect(sourcesModel, &SourcesModel::rowsRemoved,
          this, &SourcesView::hideEditor);

  /* Fully expand by default everything new file that appear: */
  sourcesList->expandAll();
  connect(sourcesModel, &SourcesModel::rowsInserted,
          this, &SourcesView::expandRows);
}

void SourcesView::showIndex(QModelIndex const &index)
{
  if (! index.isValid()) return;

  SourcesModel::TreeItem const *item =
    static_cast<SourcesModel::TreeItem const *>(index.internalPointer());
  SourcesModel::FileItem const *file =
    dynamic_cast<SourcesModel::FileItem const *>(item);
  if (file) showFile(file->sourceKey);
}

void SourcesView::showFile(conf::Key const &key)
{
  editor->setKey(key);
  rightLayout->setCurrentIndex(editorIndex);
}

void SourcesView::hideFile()
{
  editor->setKey(conf::Key::null);
  rightLayout->setCurrentIndex(noSelectionIndex);
}

void SourcesView::openInfo(QModelIndex const &index)
{
  conf::Key const infoKey =
    changeSourceKeyExt(sourcesModel->keyOfIndex(index), "info");

  KValue const *kv = nullptr;
  conf::kvs_lock.lock_shared();
  if (conf::kvs.contains(infoKey)) kv = &conf::kvs[infoKey].kv;
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
  QString const baseName =
    baseNameOfKey(sourcesModel->keyOfIndex(index));
  NewProgramDialog *dialog = new NewProgramDialog(baseName);
  dialog->show();
  dialog->raise();
}

void SourcesView::expandRows(QModelIndex const &parent, int first, int last)
{
  SourcesModel::TreeItem const *item =
    static_cast<SourcesModel::TreeItem const *>(parent.internalPointer());
  // If it's a file there is nothing to expand further:
  if (! item || ! item->isDir()) return;

  if (verbose)
    std::cout << "SourcesView: Expanding children of "
              << item->name.toStdString()
              << " from rows " << first << " to " << last << std::endl;

  sourcesList->setExpanded(parent, true);

  for (int r = first; r <= last; r ++) {
    QModelIndex const index = parent.model()->index(r, 0, parent);
    // recursively:
    int const numChildren = index.model()->rowCount(index);
    expandRows(index, 0, numChildren - 1);
  }
}

void SourcesView::hideEditor(QModelIndex const &parent, int first, int last)
{
  SourcesModel::TreeItem const *item =
    static_cast<SourcesModel::TreeItem const *>(parent.internalPointer());
  if (! item) return;

  if (item->isDir()) {
    // propagates the signal down toward the files
    for (int r = first; r <= last; r ++) {
      QModelIndex const index = parent.model()->index(r, 0, parent);
      int const numChildren = index.model()->rowCount(index);
      hideEditor(index, 0, numChildren - 1);
    }
  } else {
    /* This is a file, let's check its sourceKey is not the source that's
     * currently opened in the editor: */
    SourcesModel::FileItem const *file =
      dynamic_cast<SourcesModel::FileItem const *>(item);
    assert(file);

    if (verbose)
      std::cout << "SourcesView: File " << file->sourceKey << " deleted" << std::endl;

    if (editor && file->sourceKey == editor->textKey) {
      if (verbose) std::cout << "SourcesView: ...and sure it is!" << std::endl;
      hideFile();
    }
  }
}
