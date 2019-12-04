#include <QGridLayout>
#include <QListView>
#include <QStringListModel>
#include "Logger.h"
#include "LoggerView.h"
#include "LoggerWin.h"

LoggerWin::LoggerWin(QWidget *parent) :
  SavedWindow("LoggerWindow", "RmAdmin logs", true, parent)
{
  logger = new Logger(this);

  loggerView = new LoggerView(this);
  loggerView->setEditTriggers(QAbstractItemView::NoEditTriggers);
  setCentralWidget(loggerView);

  connect(logger, &Logger::newMessage,
          loggerView, &LoggerView::addLog);
}
