#include <QCoreApplication>
#include <QKeySequence>
#include "AboutDialog.h"
#include "ConfTreeDialog.h"
#include "ProcessesDialog.h"
#include "RCEditorDialog.h"
#include "NewSourceDialog.h"
#include "NewProgramDialog.h"
#include "menu.h"

QMenuBar *globalMenuBar;

static AboutDialog *aboutDialog;
static ConfTreeDialog *confTreeDialog;
static NewSourceDialog *newSourceDialog;
static NewProgramDialog *newProgramDialog;
static ProcessesDialog *processesDialog;
static RCEditorDialog *rcEditorDialog;

void setupGlobalMenu(GraphModel *graphModel, bool with_beta_features)
{
  // A single menubar for all windows:
  globalMenuBar = new QMenuBar(nullptr);

  /* Where we can create sources, programs, edit the running config,
   * setup storage... Everything that's editing the configuration
   * in a user friendly way. */
  QMenu *fileMenu = globalMenuBar->addMenu(
    QCoreApplication::translate("QMenuBar", "&File"));

  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "New Source…"), []() {
      if (! newSourceDialog) newSourceDialog = new NewSourceDialog;
      newSourceDialog->show();
    },
    QKeySequence::New
  );

  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "New Program…"), []() {
      if (! newProgramDialog) newProgramDialog = new NewProgramDialog;
      newProgramDialog->show();
    },
    Qt::CTRL|Qt::Key_R // _R_un
  );

  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Processes"));

  /* Where we can manage the windows and ask for specialized views
   * such as the raw editor, the graph view or other such tools: */
  QMenu *windowMenu = globalMenuBar->addMenu(
    QCoreApplication::translate("QMenuBar", "&Window"));

  /* The list of all running processes, as a qtree, equivalent to the
   * `ramen ps` command, but nicer and with stats push all the way: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Processes…"), [graphModel]() {
      if (! processesDialog) processesDialog = new ProcessesDialog(graphModel);
      processesDialog->show();
      processesDialog->raise();
    }
  );

  /* The TargetConfig editor: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Running Configuration…"), []() {
      if (! rcEditorDialog) rcEditorDialog = new RCEditorDialog;
      rcEditorDialog->show();
      rcEditorDialog->raise();
    }
  );

  /* As a last resort, a raw edition window: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Raw Configuration…"), []() {
      if (! confTreeDialog) confTreeDialog = new ConfTreeDialog;
      confTreeDialog->show();
      confTreeDialog->raise();
    }
  );

  /* An "About" entry added in any menu (but not directly in the top menubar)
   * will be moved into the automatic application menu in MacOs: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "About"), []() {
      if (! aboutDialog) aboutDialog = new AboutDialog;
      aboutDialog->show();
      aboutDialog->raise();
    }
  );

  if (with_beta_features) {
    QMenu *dashboardMenu = globalMenuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Dashboard"));
    (void)dashboardMenu;

    QMenu *alertMenu = globalMenuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Alert"));
    (void)alertMenu;
  }
}
