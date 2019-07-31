#include <QCoreApplication>
#include "AboutDialog.h"
#include "ConfTreeDialog.h"
#include "NewSourceDialog.h"
#include "menu.h"

QMenuBar *globalMenuBar;

void setupGlobalMenu(bool with_beta_features)
{
  // A single menubar for all windows:
  globalMenuBar = new QMenuBar(nullptr);

  AboutDialog *aboutWin = new AboutDialog();
  ConfTreeDialog *confTreeDialog = new ConfTreeDialog();
  NewSourceDialog *newSourceDialog = new NewSourceDialog();

  /* Where we can create sources, programs, edit the running config,
   * setup storage... Everything that's editing the configuration
   * in a user friendly way. */
  QMenu *fileMenu = globalMenuBar->addMenu(
    QCoreApplication::translate("QMenuBar", "&File"));

  QAction *newSource = fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "New Source…"),
    newSourceDialog, &NewSourceDialog::show);
  newSource->setShortcut(Qt::CTRL|Qt::Key_N);

  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "New Program…"));

  fileMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Processes"));

  /* Where we can manage the windows and ask for specialized views
   * such as the raw editor, the graph view or other such tools: */
  QMenu *windowMenu = globalMenuBar->addMenu(
    QCoreApplication::translate("QMenuBar", "&Window"));

  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "Raw Configuration"),
    confTreeDialog, &ConfTreeDialog::show);

  /* An "About" entry added in any menu (but not directly in the top menubar)
   * will be moved into the automatic application menu in MacOs: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "About"),
    aboutWin, &AboutDialog::show);

  if (with_beta_features) {
    QMenu *dashboardMenu = globalMenuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Dashboard"));
    (void)dashboardMenu;

    QMenu *alertMenu = globalMenuBar->addMenu(
      QCoreApplication::translate("QMenuBar", "&Alert"));
    (void)alertMenu;
  }
}
