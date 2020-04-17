#include <cstdlib>
#include <iostream>
#include <QMessageLogContext>
#include <QString>
#include "Logger.h"
#include "LoggerWin.h"
#include "Menu.h"

QString const logLevel(QtMsgType type)
{
  switch (type) {
    case QtDebugMsg: return "Debug";
    case QtInfoMsg: return "Info";
    case QtWarningMsg: return "Warning";
    case QtCriticalMsg: return "Critical";
    case QtFatalMsg: return "Fatal";
  }
  return "INVALID TYPE";
}

static void messageHandler(
  QtMsgType type, QMessageLogContext const &, QString const &msg)
{
  // TODO: race condition here
  if (type != QtDebugMsg && type != QtFatalMsg &&
      Menu::loggerWin && Menu::loggerWin->logger) {
    emit Menu::loggerWin->logger->newMessage(type, msg);
  } else {
    std::cerr << logLevel(type).toStdString() << ": " << msg.toStdString() << std::endl;
  }

  if (type == QtFatalMsg) abort();
}

Logger::Logger(QObject *parent) :
  QObject(parent)
{
  qInstallMessageHandler(messageHandler);
}

Logger::~Logger()
{
  qInstallMessageHandler(0);
}
