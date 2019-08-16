#include <iostream>
#include <QApplication>
#include <QCoreApplication>
#include <QMenuBar>
#include <QKeySequence>
#include "AboutDialog.h"
#include "ConfTreeDialog.h"
#include "ProcessesDialog.h"
#include "RCEditorDialog.h"
#include "NewSourceDialog.h"
#include "NewProgramDialog.h"
#include "RmAdminWin.h"
#include "SavedWindow.h"
#include "Menu.h"

static bool const verbose = true;

AboutDialog *Menu::aboutDialog;
RmAdminWin *Menu::sourceEditor;
ConfTreeDialog *Menu::confTreeDialog;
NewSourceDialog *Menu::newSourceDialog;
NewProgramDialog *Menu::newProgramDialog;
ProcessesDialog *Menu::processesDialog;
RCEditorDialog *Menu::rcEditorDialog;

void Menu::initDialogs()
{
  if (verbose) std::cout << "Create SourceEditor..." << std::endl;
  if (! sourceEditor) sourceEditor = new RmAdminWin;
  if (verbose) std::cout << "Create ConfTreeDialog..." << std::endl;
  if (! confTreeDialog) confTreeDialog = new ConfTreeDialog;
  if (verbose) std::cout << "Create NewSourceDialog..." << std::endl;
  if (! newSourceDialog) newSourceDialog = new NewSourceDialog;
  if (verbose) std::cout << "Create NewProgramDialog..." << std::endl;
  if (! newProgramDialog) newProgramDialog = new NewProgramDialog;
  if (verbose) std::cout << "Create ProcessesDialog..." << std::endl;
  if (! processesDialog) processesDialog = new ProcessesDialog;
  if (verbose) std::cout << "Create RCEditorDialog..." << std::endl;
  if (! rcEditorDialog) rcEditorDialog = new RCEditorDialog;
}

void Menu::deleteDialogs()
{
# define DELDANCE(t, w) do { \
    if (w) { \
      t *tmp = w; \
      w = nullptr; \
      delete tmp; \
    } \
  } while(0)

  DELDANCE(AboutDialog, aboutDialog);
  DELDANCE(RmAdminWin, sourceEditor);
  DELDANCE(ConfTreeDialog, confTreeDialog);
  DELDANCE(NewSourceDialog, newSourceDialog);
  DELDANCE(NewProgramDialog, newProgramDialog);
  DELDANCE(ProcessesDialog, processesDialog);
  DELDANCE(RCEditorDialog, rcEditorDialog);
}

Menu::Menu(bool with_beta_features, QMainWindow *mainWindow) :
  QObject(nullptr)
{
  // A single menubar for all windows:
  menuBar = mainWindow->menuBar();

  /* Where we can create sources, programs, edit the running config,
   * setup storage... Everything that's editing the configuration
   * in a user friendly way. */
  QMenu *fileMenu = menuBar->addMenu(
    QCoreApplication::translate("QMenuBar", "&File"));

  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "New Source…"),
    this, &Menu::openNewSourceDialog,
    QKeySequence::New);

  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "New Program…"),
    this, &Menu::openNewProgram,
    Qt::CTRL|Qt::Key_R); // _R_un

  fileMenu->addSeparator();
  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Quit"),
    this, &Menu::prepareQuit,
    Qt::CTRL|Qt::Key_Q); // _Q_uit

  /* Where we can manage the windows and ask for specialized views
   * such as the raw editor, the graph view or other such tools: */
  QMenu *windowMenu = menuBar->addMenu(
    QCoreApplication::translate("QMenuBar", "&Window"));

  /* The code editor (also the initial window) */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Source Editor…"),
    this, &Menu::openSourceEditor);

  /* The list of all running processes, as a qtree, equivalent to the
   * `ramen ps` command, but nicer and with stats push all the way: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Processes…"),
    this, &Menu::openProcesses);

  /* The TargetConfig editor: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Running Configuration…"),
    this, &Menu::openRCEditor);

  /* As a last resort, a raw edition window: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Raw Configuration…"),
    this, &Menu::openConfTreeDialog);

  /* An "About" entry added in any menu (but not directly in the top menubar)
   * will be moved into the automatic application menu in MacOs: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "About"),
    this, &Menu::openAboutDialog);

  if (with_beta_features) {
    QMenu *dashboardMenu = menuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Dashboard"));
    (void)dashboardMenu;

    QMenu *alertMenu = menuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Alert"));
    (void)alertMenu;
  }
}

void Menu::openNewSourceDialog()
{
  newSourceDialog->show();
  newSourceDialog->raise();
}

void Menu::openNewProgram()
{
  newProgramDialog->show();
  newProgramDialog->raise();
}

void Menu::openSourceEditor()
{
  sourceEditor->show();
  sourceEditor->raise();
}

void Menu::openProcesses()
{
  processesDialog->show();
  processesDialog->raise();
}

void Menu::openRCEditor()
{
  rcEditorDialog->show();
  rcEditorDialog->raise();
}

void Menu::openConfTreeDialog()
{
  confTreeDialog->show();
  confTreeDialog->raise();
}

void Menu::openAboutDialog()
{
   if (! aboutDialog) aboutDialog = new AboutDialog;
   aboutDialog->show();
   aboutDialog->raise();
}

void Menu::prepareQuit()
{
  saveWindowVisibility = true;
  qApp->closeAllWindows();
}
