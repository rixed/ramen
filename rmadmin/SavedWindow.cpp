#include <cstdlib>
#include <QCloseEvent>
#include <QCoreApplication>
#include <QSettings>
#include "Menu.h"
#include "SourcesWin.h" // for SOURCE_EDITOR_WINDOW_NAME

#include "SavedWindow.h"

SavedWindow::SavedWindow(
  QString const &windowName_,
  QString const &windowTitle,
  bool fullMenu,
  QWidget *parent,
  std::optional<bool> visibility) :
    QMainWindow(parent),
    windowName(windowName_)
{
  setUnifiedTitleAndToolBarOnMac(true);
  setWindowTitle(windowTitle);

  show();

  QSettings settings;

  settings.beginGroup(windowName);
  restoreGeometry(settings.value("geometry", saveGeometry()).toByteArray());
  restoreState(settings.value("state", saveState()).toByteArray());
  move(settings.value("position", pos()).toPoint());
  resize(settings.value("size", size()).toSize());
  if (settings.value("maximized", isMaximized()).toBool()) showMaximized();

  /* For now, make it so that the code editor is visible by default at start. */

  bool const isVisible {
    visibility.value_or(
      settings.value("visible",
                     windowName == SOURCE_EDITOR_WINDOW_NAME).toBool()) };
  setVisible(isVisible);

  settings.endGroup();

  menu = new Menu(fullMenu, this);
}

bool saveWindowVisibility = false;

void SavedWindow::closeEvent(QCloseEvent *event)
{
  QSettings settings;

  settings.beginGroup(windowName);

  settings.setValue("geometry", saveGeometry());
  settings.setValue("state", saveState());
  settings.setValue("maximized", isMaximized());
  if (! isMaximized()) {
    settings.setValue("position", pos());
    settings.setValue("size", size());
  }

  /* Obviously, when we arive here, the window is awlays visible, but
   * something want it closed. If that's the user (clicking on the window
   * close button) then save that it should not be visible. But if that's
   * the program exiting then save that it should be visible next time.
   * We distinguish because of the saveWindowVisibility flag that's set
   * when the use choose to Quit (in the menu or through the shortcut) */
  settings.setValue("visible", saveWindowVisibility);

  settings.endGroup();

  QMainWindow::closeEvent(event);
}
