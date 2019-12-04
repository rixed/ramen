#ifndef MENU_H_190731
#define MENU_H_190731
#include <QObject>

class QMainWindow;
class QMenuBar;
class AboutDialog;
class ConfTreeDialog;
class NewSourceDialog;
class NewProgramDialog;
class ProcessesDialog;
class RCEditorDialog;
class SourcesWin;
class NamesTreeWin;
class StorageWin;
class ServerInfoWin;
class OperationsWin;
class LoginWin;
class LoggerWin;

/* We need some slots to open the windows from various places, therefore
 * we need a Q_OBJECT.
 * Of course we only ever want a single instance of it: */

class QMenu;

class Menu : public QObject
{
  Q_OBJECT

  QMenu *fileMenu, *windowMenu, *dashboardMenu, *alertMenu;

  void populateMenu(bool, bool);
  void showSomething();

public:
  QMenuBar *menuBar;
  bool fullMenu;
  bool withBetaFeatures;

  static AboutDialog *aboutDialog;
  static SourcesWin *sourcesWin;
  static ConfTreeDialog *confTreeDialog;
  static NewSourceDialog *newSourceDialog;
  static NewProgramDialog *newProgramDialog;
  static ProcessesDialog *processesDialog;
  static RCEditorDialog *rcEditorDialog;
  static NamesTreeWin *namesTreeWin;
  static StorageWin *storageWin;
  static ServerInfoWin *serverInfoWin;
  static OperationsWin *operationsWin;
  static LoginWin *loginWin;
  static LoggerWin *loggerWin;

  static void initDialogs(QString const &srvUrl);
  static void initLoginWin(QString const &configDir);
  static void deleteDialogs();

  Menu(bool fullMenu, bool withBetaFeatures, QMainWindow *);

public slots:
  void upgradeToFull();  // and show something
  static void openNewSourceDialog();
  static void openNewProgram();
  static void openSourceEditor();
  static void openProcesses();
  static void openRCEditor();
  static void openConfTreeDialog();
  static void openAboutDialog();
  static void openNamesTreeWin();
  static void openStorageWin();
  static void openServerInfoWin();
  static void openOperationsWin();
  static void openLoginWin();
  static void openLoggerWin();
  static void prepareQuit();
};

#endif
