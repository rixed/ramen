#include <QGridLayout>
#include <QListView>
#include <QStringListModel>
#include "Logger.h"
#include "LoggerView.h"
#include "LogModel.h"
#include "LoggerWin.h"

LoggerWin::LoggerWin(QWidget *parent) :
  SavedWindow("LoggerWindow", "RmAdmin logs", true, parent)
{
  LogModel *logModel = new LogModel(this);

  logger = new Logger(this);

  loggerView = new LoggerView(this);
  loggerView->setModel(logModel);
  setCentralWidget(loggerView);

  connect(logger, &Logger::newMessage,
          logModel, &LogModel::append);
}
