#include <QGridLayout>
#include <QListView>
#include <QStringListModel>

#include "Logger.h"
#include "LoggerView.h"
#include "LoggerWin.h"

LoggerWin::LoggerWin(QWidget *parent) :
 SavedWindow("LoggerWindow", "RmAdming loggs", false, parent)
{
  logger = new Logger;
  loggerView = new LoggerView(this);
  loggerView->setEditTriggers(QAbstractItemView::NoEditTriggers);
  setCentralWidget(loggerView);

  connect(logger, &Logger::message,
          loggerView, &LoggerView::addLog);
}
