#include <QCoreApplication>
#include <QSettings>
#include <QCloseEvent>
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
