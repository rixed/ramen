#include <QtGlobal>
#include <QDebug>
#include <QScrollBar>
#include <QSplitter>
#include <QKeyEvent>
#include <QHeaderView>
#include <QLabel>
#include <QStackedLayout>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QComboBox>
#include "conf.h"
#include "misc.h"
#include "CodeEditForm.h"
#include "CodeEdit.h"
#include "AtomicForm.h"
#include "widgetTools.h"
#include "ButtonDelegate.h"
#include "NewProgramDialog.h"
#include "ConfTreeEditorDialog.h"
#include "SourcesModel.h"
#include "SourcesView.h"

static bool const verbose(false);

SourcesTreeView::SourcesTreeView(QWidget *parent) :
  QTreeView(parent) {}

void SourcesTreeView::keyPressEvent(QKeyEvent *event)
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

SourcesView::SourcesView(SourcesModel *sourceModel_, QWidget *parent) :
  QSplitter(parent), sourcesModel(sourceModel_)
{
  sourcesList = new SourcesTreeView(this);
  sourcesList->setObjectName("sourcesList");
  sourcesList->setModel(sourcesModel);
  sourcesList->setHeaderHidden(true);
  sourcesList->setUniformRowHeights(true);
  sourcesList->setMouseTracking(true);  // for the buttons to follow the mouse
  sourcesList->header()->setStretchLastSection(false);
  sourcesList->header()->setSectionResizeMode(0, QHeaderView::Stretch);
  sourcesList->header()->setDefaultSectionSize(30);
  sourcesList->header()->setMinimumSectionSize(30);
  sourcesList->header()->setSectionResizeMode(0, QHeaderView::ResizeToContents);
  for (int c = 1; c < SourcesModel::NumColumns; c ++) {
    sourcesList->header()->setSectionResizeMode(c, QHeaderView::Fixed);
  }
  sourcesList->setMinimumWidth(250);
  sourcesList->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
  sourcesList->setVerticalScrollBarPolicy(Qt::ScrollBarAlwaysOn);

  /* Note: delegates are not owned by the QTreeView, so let's make this the
   * owner: */
  ButtonDelegate *detailButton = new ButtonDelegate(3, this);
  sourcesList->setItemDelegateForColumn(SourcesModel::Action1, detailButton);
  connect(detailButton, &ButtonDelegate::clicked,
          this, &SourcesView::openInfo);
  ButtonDelegate *runButton = new ButtonDelegate(3, this);
  sourcesList->setItemDelegateForColumn(SourcesModel::Action2, runButton);
  connect(runButton, &ButtonDelegate::clicked,
          this, &SourcesView::runSource);

  sourcesList->resizeColumnToContents(SourcesModel::Action1);
  sourcesList->resizeColumnToContents(SourcesModel::Action2);
  sourcesList->horizontalScrollBar()->setEnabled(false);
  /* Avoid the vertical scrollbar to overwrite the last button by
   * making the qtreeview larger. FIXME or FIXQT */
  sourcesList->setMinimumWidth(
    sourcesList->width() +
    /* No idea why this has to be doubled: */
    2 * sourcesList->verticalScrollBar()->width());

  addWidget(sourcesList);
  setStretchFactor(0, 0);

  rightLayout = new QStackedLayout;

  editorForm = new CodeEditForm;
  editorForm->setObjectName("editorForm");
  codeEditorIndex = rightLayout->addWidget(editorForm);

  noSelection =
    new QLabel(tr("Select a source file on the left to view/edit it."));
  noSelection->setObjectName("noSelection");
  noSelection->setWordWrap(true);
  noSelection->setAlignment(Qt::AlignCenter);
  noSelectionIndex = rightLayout->addWidget(noSelection);
  rightLayout->setCurrentIndex(noSelectionIndex);

  QWidget *rightPanel = new QWidget;
  rightPanel->setObjectName("rightPanel");
  rightPanel->setLayout(rightLayout);
  addWidget(rightPanel);
  setStretchFactor(1, 1);

  // Connect selection of a program to the display of its code:
  connect(sourcesList, &SourcesTreeView::activated,
          this, &SourcesView::showIndex);
  connect(sourcesList, &SourcesTreeView::clicked,
          this, &SourcesView::showIndex);

  /* Connect the edition start/stop of the code to disabling/reenabling selection
   * in the QTreeWidget: */
  connect(editorForm, &AtomicForm::changeEnabled,
          sourcesList, &SourcesTreeView::setDisabled);
  // TODO: same for the alertInfoEditor

  /* Connect the deletion of a source to hiding the editorForm if that's the
   * current source: */
  connect(sourcesModel, &SourcesModel::rowsAboutToBeRemoved,
          this, &SourcesView::hideEditor);
  // TODO: same for the alertInfoEditor

  /* Fully expand by default every new file that appear: */
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
  if (file) {
    editorForm->codeEdit->setKeyPrefix(file->sourceKeyPrefix);
    rightLayout->setCurrentIndex(codeEditorIndex);
    // Also make sure it is selected in the left treeview:
    sourcesList->setCurrentIndex(index);
  }
}

void SourcesView::showFile(std::string const &keyPrefix)
{
  QModelIndex const index = sourcesModel->indexOfKeyPrefix(keyPrefix);
  if (! index.isValid()) {
    qWarning() << "Cannot find the QModelIndex of key prefix"
               << QString::fromStdString(keyPrefix);
    return;
  }

  showIndex(index);
}

void SourcesView::hideFile()
{
  rightLayout->setCurrentIndex(noSelectionIndex);
}

void SourcesView::openInfo(QModelIndex const &index)
{
  std::string const infoKey =
    sourcesModel->keyPrefixOfIndex(index) + "/info";

  ConfTreeEditorDialog *dialog = new ConfTreeEditorDialog(infoKey);
  dialog->show();
}

void SourcesView::runSource(QModelIndex const &index)
{
  SourcesModel::TreeItem const *item =
    static_cast<SourcesModel::TreeItem const *>(index.internalPointer());
  QString const baseName = item->fqName();

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
    qDebug() << "SourcesView: Expanding children of"
             << item->name
             << "from rows" << first << "to" << last;

  expandAllFromParent(sourcesList, parent, first, last);
}

void SourcesView::hideEditor(QModelIndex const &parent, int first, int last)
{
  if (verbose)
    qDebug() << "SourcesView::hideEditor: Removing rows" << first << ".."
             << last;

  for (int r = first ; r <= last ; r ++) {
    QModelIndex const i = sourcesList->model()->index(r, 0, parent);
    SourcesModel::TreeItem const *item =
      static_cast<SourcesModel::TreeItem const *>(i.internalPointer());

    if (! item) {
      qCritical() << "Row" << r << "is not a TreeItem!?";
      return;
    }

    if (item->isDir()) {
      hideEditor(i, 0, sourcesList->model()->rowCount(i) - 1);
    } else {
      /* This is a file, let's check its sourceKey is not the source that's
       * currently opened in the editorForm: */
      SourcesModel::FileItem const *file =
        dynamic_cast<SourcesModel::FileItem const *>(item);
      assert(file);

      if (verbose)
        qDebug() << "SourcesView: File"
                 << QString::fromStdString(file->sourceKeyPrefix) << "deleted";

      if (editorForm && file->sourceKeyPrefix == editorForm->codeEdit->keyPrefix) {
        hideFile();
      }
    }
  }
}
