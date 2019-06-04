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
  /* We want the treeview on the left with all the source files and
   * info.
   * Sometime we will have a text file but no object and sometime the
   * other way around. In any case there should be an entry in the
   * SourcesModel and in the treeview.
   * The model should also show the current permissions of the text
   * and object files, those that are locked and by whom.
   * We could also have an option to hide "system" programs, or others
   * programs, etc.
   *.
   * When one is selected, it is also opened on the right tabbar.
   * The tab panel for a source file has 3 areas:
   *
   * - on the left, a fixed width panel with this source info
   *   and a list of running programs using that source (for each,
   *   the name and the parameters). This form is editable by the way
   *   to update running (or disabled) programs.
   *   If there is no info file the panel just says so.
   *   If the compilation is OK, a small form allows to run that
   *   program. The form ask for all parameters, debug mode or not,
   *   reporting period, site filter, and most importantly the name of the program
   *   it's going to run as. Also a checkbox to "disable", ie. an RC
   *   entry will be saved (thus saving the parameter values and name etc)
   *   but no actual program started.
   *
   * - on the right, the rest is for the text editor. If there is
   *   no source code any mode the panel just says so (note: we
   *   could recreate a source code from the object operation,
   *   with a comment in the header explaining the situation.
   *
   * - on the bottom of the text editor, a button bar with
   *   "clone" -> open a popup to select a name and location.
   *              that will not do anything unless we then edit and save
   *              though.
   *   "edit" -> that lock the source and turns the text editor
   *             into edition mode.
   *   "save" -> write the file (only once in edition) and unlock it.
   *   When a new source file is saved the GUI notice that the MD5 of the
   *   info panel does not match the text and therefore blur that panel,
   *   until a new version of the info with the proper md5 is received.
   *   "delete" -> confirmation dialog and then do delete.
   *               If that source is used by a running program the dialog
   *               should warn though. But technically all we need is the
   *               info to run a program.
   */

  sourcesList = new QTreeView(this);
  sourcesList->setModel(sourcesModel);
  sourcesList->setHeaderHidden(true);
  sourcesList->setUniformRowHeights(true);
  setStretchFactor(0, 0);

  sourceTabs = new QTabWidget(this);
  sourceTabs->setTabsClosable(true);
  setStretchFactor(1, 1);

  // Connect selection of a program to showing its code:
  connect(sourcesList, &QAbstractItemView::activated,
          this, [this](QModelIndex const &index) {
    if (! index.isValid()) return;

    SourcesModel::TreeItem const *item =
      static_cast<SourcesModel::TreeItem const *>(index.internalPointer());
    SourcesModel::FileItem const *file =
      dynamic_cast<SourcesModel::FileItem const *>(item);
    if (file) showFile(file->origKey);
  });
}

void SourcesView::showFile(conf::Key const &key)
{
  QString label = QString::fromStdString(key.s); // TODO: extract the actual program name
  if (tryFocusTab(sourceTabs, label)) return;
  CodeEdit *editor = new CodeEdit(key);
  sourceTabs->addTab(editor, label);
  focusLastTab(sourceTabs);
}
