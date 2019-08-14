#include <cstdlib>
#include <QCoreApplication>
#include <QSettings>
#include <QCloseEvent>
#include "Menu.h"
#include "SavedWindow.h"

SavedWindow::SavedWindow(
  QString const &windowName_, QString const &windowTitle, QWidget *parent) :
  QMainWindow(parent),
  windowName(windowName_)
{
  setUnifiedTitleAndToolBarOnMac(true);
  setWindowTitle(windowTitle);

  QSettings settings(QCoreApplication::organizationName(),
                     QCoreApplication::applicationName());

  settings.beginGroup(windowName);
  restoreGeometry(settings.value("geometry", saveGeometry()).toByteArray());
  restoreState(settings.value("state", saveState()).toByteArray());
  move(settings.value("position", pos()).toPoint());
  resize(settings.value("size", size()).toSize());
  if (settings.value("maximized", isMaximized()).toBool()) showMaximized();
  settings.endGroup();

  bool with_beta_features = getenv("RMADMIN_BETA");
  menu = new Menu(with_beta_features, this);
}

void SavedWindow::closeEvent(QCloseEvent *event)
{
  QSettings settings(QCoreApplication::organizationName(),
                     QCoreApplication::applicationName());

  settings.beginGroup(windowName);

  settings.setValue("geometry", saveGeometry());
  settings.setValue("state", saveState());
  settings.setValue("maximized", isMaximized());
  if (! isMaximized()) {
    settings.setValue("position", pos());
    settings.setValue("size", size());
  }

  settings.endGroup();

  QMainWindow::closeEvent(event);
}
