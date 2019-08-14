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
#include "Menu.h"

AboutDialog *Menu::aboutDialog;
ConfTreeDialog *Menu::confTreeDialog;
NewSourceDialog *Menu::newSourceDialog;
NewProgramDialog *Menu::newProgramDialog;
ProcessesDialog *Menu::processesDialog;
RCEditorDialog *Menu::rcEditorDialog;

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
    this, &Menu::openSourceDialog,
    QKeySequence::New);

  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "New Program…"),
    this, &Menu::openNewProgram,
    Qt::CTRL|Qt::Key_R); // _R_un

  fileMenu->addSeparator();
  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Quit"),
    qApp, &QApplication::closeAllWindows,
    Qt::CTRL|Qt::Key_Q); // _Q_uit

  /* Where we can manage the windows and ask for specialized views
   * such as the raw editor, the graph view or other such tools: */
  QMenu *windowMenu = menuBar->addMenu(
    QCoreApplication::translate("QMenuBar", "&Window"));

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

void Menu::openSourceDialog()
{
  if (! newSourceDialog) newSourceDialog = new NewSourceDialog;
  newSourceDialog->show();
  newSourceDialog->raise();
}

void Menu::openNewProgram()
{
  if (! newProgramDialog) newProgramDialog = new NewProgramDialog;
  newProgramDialog->show();
  newProgramDialog->raise();
}

void Menu::openProcesses()
{
  if (! processesDialog) processesDialog = new ProcessesDialog();
  processesDialog->show();
  processesDialog->raise();
}

void Menu::openRCEditor()
{
  if (! rcEditorDialog) rcEditorDialog = new RCEditorDialog;
  rcEditorDialog->show();
  rcEditorDialog->raise();
}

void Menu::openConfTreeDialog()
{
  if (! confTreeDialog) confTreeDialog = new ConfTreeDialog;
  confTreeDialog->show();
  confTreeDialog->raise();
}

void Menu::openAboutDialog()
{
   if (! aboutDialog) aboutDialog = new AboutDialog;
   aboutDialog->show();
   aboutDialog->raise();
}
