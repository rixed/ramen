#include <iostream>
#include <QMessageLogContext>
#include <QString>
#include "Logger.h"
#include "LoggerWin.h"
#include "Menu.h"

static void messageHandler(
  QtMsgType type, const QMessageLogContext& context, const QString& msg)
{
  QByteArray localMsg = msg.toLocal8Bit();
  const char *file = context.file ? context.file : "";
  const char *function = context.function ? context.function : "";

  // TODO: race condition here
  if (Menu::loggerWin && Menu::loggerWin->logger) {
    emit Menu::loggerWin->logger->newMessage(msg);
  } else {
    switch (type) {
      case QtDebugMsg:
        fprintf(stderr, "Debug: %s (%s:%u, %s)\n", localMsg.constData(), file, context.line, function);
        break;
      case QtInfoMsg:
        fprintf(stderr, "Info: %s (%s:%u, %s)\n", localMsg.constData(), file, context.line, function);
        break;
      case QtWarningMsg:
        fprintf(stderr, "Warning: %s (%s:%u, %s)\n", localMsg.constData(), file, context.line, function);
        break;
      case QtCriticalMsg:
        fprintf(stderr, "Critical: %s (%s:%u, %s)\n", localMsg.constData(), file, context.line, function);
        break;
      case QtFatalMsg:
        fprintf(stderr, "Fatal: %s (%s:%u, %s)\n", localMsg.constData(), file, context.line, function);
        break;
    }
  }
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
