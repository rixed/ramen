#ifndef MENU_H_190731
#define MENU_H_190731
#include <string>
#include <QObject>
#include "conf.h"

class AboutDialog;
class ConfTreeDialog;
struct KValue;
class LoggerWin;
class LoginWin;
class NamesTreeWin;
class NewDashboardDialog;
class NewProgramDialog;
class NewSourceDialog;
class OperationsWin;
class ProcessesDialog;
class QMainWindow;
class QMenuBar;
class RCEditorDialog;
class ServerInfoWin;
class SourcesWin;
class StorageWin;
class AlertingWin;

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

  // Add a dashboard in the dashboard menu:
  void addDashboard(QString const &, std::string const &key_prefix);
  void addValue(std::string const &, KValue const &);
  void delValue(std::string const &, KValue const &);

public:
  QMenuBar *menuBar;
  bool fullMenu;

  static AboutDialog *aboutDialog;
  static SourcesWin *sourcesWin;
  static ConfTreeDialog *confTreeDialog;
  static NewSourceDialog *newSourceDialog;
  static NewProgramDialog *newProgramDialog;
  static NewDashboardDialog *newDashboardDialog;
  static ProcessesDialog *processesDialog;
  static RCEditorDialog *rcEditorDialog;
  static NamesTreeWin *namesTreeWin;
  static StorageWin *storageWin;
  static ServerInfoWin *serverInfoWin;
  static OperationsWin *operationsWin;
  static LoginWin *loginWin;
  static LoggerWin *loggerWin;
  static AlertingWin *alertingWin;

  static void initDialogs(QString const &srvUrl);
  static void initLoginWin(QString const &configDir);
  static void deleteDialogs();

  Menu(bool fullMenu, QMainWindow *);

public slots:
  void upgradeToFull();  // and show something
  static void openNewSourceDialog();
  static void openNewProgramDialog();
  static void openNewDashboardDialog();
  static void openSourceEditor();
  static void openProcesses();
  static void openRCEditor();
  static void openConfTreeDialog();
  static void openAboutDialog();
  static void openNamesTreeWin();
  static void openStorageWin();
  static void openAlertingWin();
  static void openServerInfoWin();
  static void openOperationsWin();
  static void openLoginWin();
  static void openLoggerWin();
  static void prepareQuit();
  static void openDashboard(QString const &, std::string const &);

protected slots:
  void onChange(QList<ConfChange> const &);
};

#endif
