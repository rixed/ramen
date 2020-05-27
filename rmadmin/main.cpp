#include <cassert>
#include <iostream>
#include <string>
#include <cstdlib>
extern "C" {
# include <caml/mlvalues.h>
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
# include <caml/startup.h>
# include <caml/callback.h>
# include <caml/threads.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include <QApplication>
#include <QDebug>
#include <QtWidgets>
#include <QMetaType>
#include <QCommandLineParser>
#include <QVector>
#include "alerting/AlertingLogsModel.h"
#include "conf.h"
#include "dashboard/DashboardTreeModel.h"
#include "GraphModel.h"
#include "GraphViewSettings.h"
#include "Logger.h"
#include "LoginWin.h"
#include "Menu.h"
#include "NamesTree.h"
#include "SourcesWin.h"
#include "SyncStatus.h"
#include "UserIdentity.h"
#include "../src/config.h"

int main(int argc, char *argv[])
{
  caml_startup(argv);
  caml_release_runtime_system();

  QApplication app(argc, argv);
  QCoreApplication::setOrganizationName("Accedian");
  QCoreApplication::setOrganizationDomain("accedian.com");
  QCoreApplication::setApplicationName("RmAdmin");
  QCoreApplication::setApplicationVersion(PACKAGE_VERSION);

  QCommandLineParser parser;
  parser.setApplicationDescription("Ramen Client GUI");
  parser.addHelpOption();
  parser.addVersionOption();

  /* For GuiHelper: */
  QCommandLineOption debugOption(
      QString("debug"),
      QCoreApplication::translate("main", "Display confserver messages"));
  parser.addOption(debugOption);

  parser.process(app);

  QString configDir =
    getenv("HOME") ?
      qgetenv("HOME") + QString("/.config/rmadmin") :
      QString("/etc/rmadmin");

  qRegisterMetaType<std::string>();
  qRegisterMetaType<KValue>();
  qRegisterMetaType<std::shared_ptr<conf::Value const>>();
  qRegisterMetaType<conf::Error>();
  qRegisterMetaType<conf::Worker>();
  qRegisterMetaType<conf::TimeRange>();
  qRegisterMetaType<conf::Retention>();
  qRegisterMetaType<QVector<int>>();
  qRegisterMetaType<QtMsgType>();
  qRegisterMetaType<QList<ConfChange>>();
  qRegisterMetaType<SyncStatus>();

  // Creates the global kvs (store of keys) before widgets start to use it:
  kvs = new KVStore;

  /* A GraphModel satisfies both the TreeView and the GraphView
   * requirements: */
  GraphViewSettings *settings = new GraphViewSettings;
  GraphModel::globalGraphModel = new GraphModel(settings);
  NamesTree::globalNamesTree = new NamesTree(true);
  NamesTree::globalNamesTreeAnySites = new NamesTree(false);
  DashboardTreeModel::globalDashboardTree = new DashboardTreeModel;
  AlertingLogsModel::globalLogsModel = new AlertingLogsModel;

  Menu::initLoginWin(configDir);
  Menu::openLoginWin();

  int ret = app.exec();

  // forbids the OCaml thread to use the kvs:
  exiting = true;

  Menu::deleteDialogs();
  danceOfDelLater<AlertingLogsModel>(&AlertingLogsModel::globalLogsModel);
  danceOfDelLater<NamesTree>(&NamesTree::globalNamesTree);
  danceOfDelLater<GraphModel>(&GraphModel::globalGraphModel);
  danceOfDel<GraphViewSettings>(&settings);

  return ret;
}
