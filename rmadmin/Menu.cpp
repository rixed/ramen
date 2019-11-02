#include <iostream>
#include <QApplication>
#include <QCoreApplication>
#include <QMenuBar>
#include <QKeySequence>
#include "misc.h"
#include "AboutDialog.h"
#include "ConfTreeDialog.h"
#include "ProcessesDialog.h"
#include "RCEditorDialog.h"
#include "NewSourceDialog.h"
#include "NewProgramDialog.h"
#include "SourcesWin.h"
#include "SavedWindow.h"
#include "NamesTreeWin.h"
#include "StorageWin.h"
#include "ServerInfoWin.h"
#include "Menu.h"

static bool const verbose = true;

AboutDialog *Menu::aboutDialog;
SourcesWin *Menu::sourceEditor;
ConfTreeDialog *Menu::confTreeDialog;
NewSourceDialog *Menu::newSourceDialog;
NewProgramDialog *Menu::newProgramDialog;
ProcessesDialog *Menu::processesDialog;
RCEditorDialog *Menu::rcEditorDialog;
NamesTreeWin *Menu::namesTreeWin;
StorageWin *Menu::storageWin;
ServerInfoWin *Menu::serverInfoWin;

void Menu::initDialogs(QString const &srvUrl)
{
  if (verbose) std::cout << "Create SourceEditor..." << std::endl;
  if (! sourceEditor) sourceEditor = new SourcesWin;
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
  if (verbose) std::cout << "Create NamesTreeWin..." << std::endl;
  if (! namesTreeWin) namesTreeWin = new NamesTreeWin;
  if (verbose) std::cout << "Create StorageWin..." << std::endl;
  if (! storageWin) storageWin = new StorageWin;
  if (verbose) std::cout << "Create ServerInfoWin ..." << std::endl;
  if (! serverInfoWin) serverInfoWin = new ServerInfoWin(srvUrl);
}

void Menu::deleteDialogs()
{
  danceOfDel<AboutDialog>(&aboutDialog);
  danceOfDel<SourcesWin>(&sourceEditor);
  danceOfDel<ConfTreeDialog>(&confTreeDialog);
  danceOfDel<NewSourceDialog>(&newSourceDialog);
  danceOfDel<NewProgramDialog>(&newProgramDialog);
  danceOfDel<ProcessesDialog>(&processesDialog);
  danceOfDel<RCEditorDialog>(&rcEditorDialog);
  danceOfDel<NamesTreeWin>(&namesTreeWin);
  danceOfDel<StorageWin>(&storageWin);
  danceOfDel<ServerInfoWin>(&serverInfoWin);
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

  /* The Storage configuration window: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Storage Configuration…"),
    this, &Menu::openStorageWin);

  /* The Server Information window: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Server Information…"),
    this, &Menu::openServerInfoWin);

  /* As a last resort, a raw edition window: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Raw Configuration…"),
    this, &Menu::openConfTreeDialog);

  /* DEBUG: the list of all names, to test autocompletion: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Completable Names…"),
    this, &Menu::openNamesTreeWin);

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

static void showRaised(QWidget *w)
{
  w->raise();
  w->show();
}

void Menu::openNewSourceDialog()
{
  newSourceDialog->clear();
  showRaised(newSourceDialog);
}

void Menu::openNewProgram()
{
  showRaised(newProgramDialog);
}

void Menu::openSourceEditor()
{
  showRaised(sourceEditor);
}

void Menu::openProcesses()
{
  showRaised(processesDialog);
}

void Menu::openRCEditor()
{
  showRaised(rcEditorDialog);
}

void Menu::openConfTreeDialog()
{
  showRaised(confTreeDialog);
}

void Menu::openAboutDialog()
{
   if (! aboutDialog) aboutDialog = new AboutDialog;
   showRaised(aboutDialog);
}

void Menu::openNamesTreeWin()
{
  showRaised(namesTreeWin);
}

void Menu::openStorageWin()
{
  showRaised(storageWin);
}

void Menu::openServerInfoWin()
{
  showRaised(serverInfoWin);
}

void Menu::prepareQuit()
{
  saveWindowVisibility = true;
  qApp->closeAllWindows();
}
