#ifndef LOGGERWIN_H_191025
#define LOGGERWIN_H_191025
#include "SavedWindow.h"

class LoggerView;
class Logger;

class LoggerWin : public SavedWindow
{
  Q_OBJECT
  LoggerView *loggerView;

public:
  LoggerWin(QWidget *parent = nullptr);
  Logger *logger;
};

#endif

