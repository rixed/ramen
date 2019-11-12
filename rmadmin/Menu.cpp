#include <cassert>
#include <QApplication>
#include <QCoreApplication>
#include <QDebug>
#include <QMenuBar>
#include <QKeySequence>
#include "misc.h"
#include "AboutDialog.h"
#include "ConfTreeDialog.h"
#include "ProcessesDialog.h"
#include "RCEditorDialog.h"
#include "LoginWin.h"
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
SourcesWin *Menu::sourcesWin;
ConfTreeDialog *Menu::confTreeDialog;
NewSourceDialog *Menu::newSourceDialog;
NewProgramDialog *Menu::newProgramDialog;
ProcessesDialog *Menu::processesDialog;
RCEditorDialog *Menu::rcEditorDialog;
NamesTreeWin *Menu::namesTreeWin;
StorageWin *Menu::storageWin;
ServerInfoWin *Menu::serverInfoWin;
LoginWin *Menu::loginWin;

void Menu::initLoginWin(QString const &configDir)
{
  if (verbose) qDebug() << "Create LoginWin...";
  if (! loginWin) loginWin = new LoginWin(configDir);
}

void Menu::initDialogs(QString const &srvUrl)
{
  if (verbose) qDebug() << "Create SourceEditor...";
  if (! sourcesWin) sourcesWin = new SourcesWin;
  if (verbose) qDebug() << "Create ConfTreeDialog...";
  if (! confTreeDialog) confTreeDialog = new ConfTreeDialog;
  if (verbose) qDebug() << "Create NewSourceDialog...";
  if (! newSourceDialog) newSourceDialog = new NewSourceDialog;
  if (verbose) qDebug() << "Create NewProgramDialog...";
  if (! newProgramDialog) newProgramDialog = new NewProgramDialog;
  if (verbose) qDebug() << "Create ProcessesDialog...";
  if (! processesDialog) processesDialog = new ProcessesDialog;
  if (verbose) qDebug() << "Create RCEditorDialog...";
  if (! rcEditorDialog) rcEditorDialog = new RCEditorDialog;
  if (verbose) qDebug() << "Create NamesTreeWin...";
  if (! namesTreeWin) namesTreeWin = new NamesTreeWin;
  if (verbose) qDebug() << "Create StorageWin...";
  if (! storageWin) storageWin = new StorageWin;
  if (verbose) qDebug() << "Create ServerInfoWin...";
  if (! serverInfoWin) serverInfoWin = new ServerInfoWin(srvUrl);
  // login is supposed to be initialized first
  assert(loginWin);
}

void Menu::showSomething()
{
  bool someOpened = false;
  someOpened |= sourcesWin->isVisible();
  someOpened |= confTreeDialog->isVisible();
  someOpened |= processesDialog->isVisible();
  someOpened |= rcEditorDialog->isVisible();
  someOpened |= storageWin->isVisible();

  if (! someOpened) sourcesWin->show();
}

void Menu::deleteDialogs()
{
  danceOfDel<AboutDialog>(&aboutDialog);
  danceOfDel<SourcesWin>(&sourcesWin);
  danceOfDel<ConfTreeDialog>(&confTreeDialog);
  danceOfDel<NewSourceDialog>(&newSourceDialog);
  danceOfDel<NewProgramDialog>(&newProgramDialog);
  danceOfDel<ProcessesDialog>(&processesDialog);
  danceOfDel<RCEditorDialog>(&rcEditorDialog);
  danceOfDel<NamesTreeWin>(&namesTreeWin);
  danceOfDel<StorageWin>(&storageWin);
  danceOfDel<ServerInfoWin>(&serverInfoWin);
  danceOfDel<LoginWin>(&loginWin);
}

void Menu::populateMenu(bool basic, bool extended)
{
  if (basic) {
    /* Where we can create sources, programs, edit the running config,
     * setup storage... Everything that's editing the configuration
     * in a user friendly way. */
    fileMenu = menuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&File"));
  }

  if (extended) {
    assert(fileMenu);  // user is supposed to populate basic first
    fileMenu->addAction(
      QCoreApplication::translate("QMenuBar", "New Source…"),
      this, &Menu::openNewSourceDialog,
      QKeySequence::New);

    fileMenu->addAction(
      QCoreApplication::translate("QMenuBar", "New Program…"),
      this, &Menu::openNewProgram,
      Qt::CTRL|Qt::Key_R); // _R_un

    fileMenu->addSeparator();
  }

  if (basic) {
    fileMenu->addAction(
      QCoreApplication::translate("QMenuBar", "Quit"),
      this, &Menu::prepareQuit,
      Qt::CTRL|Qt::Key_Q); // _Q_uit

    /* Where we can manage the windows and ask for specialized views
     * such as the raw editor, the graph view or other such tools: */
    windowMenu = menuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Window"));

    /* The login window */
    windowMenu->addAction(
      QCoreApplication::translate("QMenuBar", "Login…"),
      this, &Menu::openLoginWin);
  }

  if (extended) {
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
  }

  if (basic) {
    /* An "About" entry added in any menu (but not directly in the top menubar)
     * will be moved into the automatic application menu in MacOs: */
    windowMenu->addAction(
      QCoreApplication::translate("QMenuBar", "About"),
      this, &Menu::openAboutDialog);
  }

  if (extended && withBetaFeatures) {
    dashboardMenu = menuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Dashboard"));
    (void)dashboardMenu;

    alertMenu = menuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Alert"));
    (void)alertMenu;

    /* DEBUG: the list of all names, to test autocompletion: */
    windowMenu->addAction(
      QCoreApplication::translate("QMenuBar", "Completable Names…"),
      this, &Menu::openNamesTreeWin);
  }
}

Menu::Menu(bool fullMenu_, bool withBetaFeatures_, QMainWindow *mainWindow) :
  QObject(nullptr),
  fullMenu(fullMenu_),
  withBetaFeatures(withBetaFeatures_)
{
  // A single menubar for all windows:
  menuBar = mainWindow->menuBar();

  populateMenu(true, fullMenu);
}

void Menu::upgradeToFull()
{
  if (fullMenu) return;
  populateMenu(false, true);
  showSomething();
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
  showRaised(sourcesWin);
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

void Menu::openLoginWin()
{
  showRaised(loginWin);
  loginWin->focusSubmit();
}

void Menu::prepareQuit()
{
  saveWindowVisibility = true;
  qApp->closeAllWindows();
}
