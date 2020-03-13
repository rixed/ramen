#include <cassert>
#include <QAction>
#include <QApplication>
#include <QCoreApplication>
#include <QDebug>
#include <QMainWindow>
#include <QMenuBar>
#include <QKeySequence>
#include "AboutDialog.h"
#include "ConfTreeDialog.h"
#include "conf.h"
#include "dashboard/DashboardWindow.h"
#include "dashboard/tools.h"
#include "LoggerView.h"
#include "LoggerWin.h"
#include "LoginWin.h"
#include "misc.h"
#include "NamesTreeWin.h"
#include "NewSourceDialog.h"
#include "NewProgramDialog.h"
#include "NewDashboardDialog.h"
#include "OperationsWin.h"
#include "ProcessesDialog.h"
#include "RCEditorDialog.h"
#include "SavedWindow.h"
#include "ServerInfoWin.h"
#include "SourcesWin.h"
#include "StorageWin.h"

#include "Menu.h"

static bool const verbose = false;

AboutDialog *Menu::aboutDialog;
SourcesWin *Menu::sourcesWin;
ConfTreeDialog *Menu::confTreeDialog;
NewSourceDialog *Menu::newSourceDialog;
NewProgramDialog *Menu::newProgramDialog;
NewDashboardDialog *Menu::newDashboardDialog;
ProcessesDialog *Menu::processesDialog;
RCEditorDialog *Menu::rcEditorDialog;
NamesTreeWin *Menu::namesTreeWin;
StorageWin *Menu::storageWin;
ServerInfoWin *Menu::serverInfoWin;
OperationsWin *Menu::operationsWin;
LoginWin *Menu::loginWin;
LoggerWin *Menu::loggerWin;

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
  if (verbose) qDebug() << "Create NewDashboardDialog...";
  if (! newDashboardDialog) newDashboardDialog = new NewDashboardDialog;
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
  if (verbose) qDebug() << "Create OperationsWin...";
  if (! operationsWin) operationsWin = new OperationsWin;
  if (verbose) qDebug() << "Create Logger ...";
  if (! loggerWin) loggerWin = new LoggerWin;
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
  danceOfDelLater<LoginWin>(&loginWin);

  danceOfDelLater<AboutDialog>(&aboutDialog);
  danceOfDelLater<SourcesWin>(&sourcesWin);
  danceOfDelLater<ConfTreeDialog>(&confTreeDialog);
  danceOfDelLater<NewSourceDialog>(&newSourceDialog);
  danceOfDelLater<NewProgramDialog>(&newProgramDialog);
  danceOfDelLater<NewDashboardDialog>(&newDashboardDialog);
  danceOfDelLater<ProcessesDialog>(&processesDialog);
  danceOfDelLater<RCEditorDialog>(&rcEditorDialog);
  danceOfDelLater<NamesTreeWin>(&namesTreeWin);
  danceOfDelLater<StorageWin>(&storageWin);
  danceOfDelLater<ServerInfoWin>(&serverInfoWin);
  danceOfDelLater<OperationsWin>(&operationsWin);
  // delLater is never when we quit the app:
  if (loggerWin) loggerWin->loggerView->flush();
  danceOfDelLater<LoggerWin>(&loggerWin);
}

/* This function can be called either once (with basic and extended) or twice
 * (first with basic, and then with extended): */
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
      this, &Menu::openNewProgramDialog,
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

    /* The graph of operations window: */
    windowMenu->addAction(
      QCoreApplication::translate("QMenuBar", "Graph of Operations…"),
      this, &Menu::openOperationsWin);

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

    /* The Logger window */
    windowMenu->addAction(
      QCoreApplication::translate("QMenuBar", "Log messages…"),
      this, &Menu::openLoggerWin);

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

  if (extended) {
    dashboardMenu = menuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Dashboard"));

    /* Number of static entries in the dashboardMenu before the list of
     * defined dashboards (separator does count as an action): */
#   define NUM_STATIC_DASHBOARD_ACTIONS 2

    dashboardMenu->addAction(
      QCoreApplication::translate("QMenuBar", "New Dashboard…"),
      this, &Menu::openNewDashboardDialog,
      Qt::CTRL|Qt::Key_D);

    dashboardMenu->addSeparator();

    /* Dynamically add new dashboards: */
    connect(&kvs, &KVStore::valueCreated,
            this, &Menu::addValue);
    connect(&kvs, &KVStore::valueDeleted,
            this, &Menu::delValue);

    /* Also populate from what we already have: */
    iterDashboards([this](std::string const &, KValue const &,
                          QString const &name, std::string const &key_prefix) {
      addDashboard(name, key_prefix);
    });
  }

  if (extended && withBetaFeatures) {
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
  if (! w) return;
  w->raise();
  w->show();
}

void Menu::openNewSourceDialog()
{
  newSourceDialog->clear();
  showRaised(newSourceDialog);
}

void Menu::openNewProgramDialog()
{
  showRaised(newProgramDialog);
}

void Menu::openNewDashboardDialog()
{
  newDashboardDialog->clear();
  showRaised(newDashboardDialog);
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

void Menu::openOperationsWin()
{
  showRaised(operationsWin);
}

void Menu::openLoginWin()
{
  showRaised(loginWin);
  loginWin->focusSubmit();
}

void Menu::openLoggerWin()
{
  showRaised(loggerWin);
}

void Menu::prepareQuit()
{
  saveWindowVisibility = true;
  qApp->closeAllWindows();
}

void Menu::addDashboard(QString const &name, std::string const &key_prefix)
{
  QAction *openDashboardAction = new QAction(name, this);
  connect(openDashboardAction, &QAction::triggered,
    /* Note to self: those captured copies are actual copies of the underlying
     * data not of the reference */
    [name, key_prefix] (bool) {
      openDashboard(name, key_prefix);
  });
  // Locate where to insert this new menu entry:
  QList<QAction *> const actions = dashboardMenu->actions();
  QAction *before = nullptr;
  for (int i = NUM_STATIC_DASHBOARD_ACTIONS; i < actions.length(); i++) {
    int const c = actions[i]->text().compare(name);
    if (c < 0) continue;
    if (c == 0) return;
    before = actions[i];
    break;
  }
  dashboardMenu->insertAction(before, openDashboardAction);
}

void Menu::addValue(std::string const &key, KValue const &)
{
  std::pair<QString const, std::string> name_prefix =
    dashboardNameAndPrefOfKey(key);
  if (!name_prefix.first.isEmpty())
      addDashboard(name_prefix.first, name_prefix.second);
  // Other dynamic menus can be completed here
}

void Menu::delValue(std::string const &key, KValue const &)
{
  QString const name = dashboardNameOfKey(key);
  if (!name.isEmpty()) {
    QList<QAction *> const actions = dashboardMenu->actions();
    for (int i = NUM_STATIC_DASHBOARD_ACTIONS; i < actions.length(); i++) {
      int const c = actions[i]->text().compare(name);
      if (c < 0) continue;
      if (c > 0) return;
      dashboardMenu->removeAction(actions[i]);
      break;
    }
  }
}

void Menu::openDashboard(QString const &name, std::string const &key_prefix)
{
  QMainWindow *w = new DashboardWindow(name, key_prefix);
  showRaised(w);
}
