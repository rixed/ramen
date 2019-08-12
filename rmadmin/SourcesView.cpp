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
  rightLayout->setCurrentIndex(
    rightLayout->addWidget(noSelection));

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
  QString const baseName =
    baseNameOfKey(sourcesModel->keyOfIndex(index));
  std::cerr << "BASENAME = " << baseName.toStdString() << std::endl;
  NewProgramDialog *dialog = new NewProgramDialog(baseName);
  dialog->show();
  dialog->raise();
}
