#include <QCoreApplication>
#include "AboutDialog.h"
#include "menu.h"

QMenuBar *globalMenuBar;

void setupGlobalMenu(bool)
{
  // A single menubar for all windows:
  globalMenuBar = new QMenuBar(nullptr);

  AboutDialog *aboutWin = new AboutDialog();

  QMenu *windowMenu = globalMenuBar->addMenu(
    QCoreApplication::translate("QMenuBar", "&Window"));

  /* An "About" entry added in any menu (but not directly in the top menubar)
   * will be moved into the automatic application menu in MacOs: */
  windowMenu->addAction(
    QCoreApplication::translate("QMenuBar", "About"),
    aboutWin, &AboutDialog::show);
}
