#ifndef LOGGER_H_191025
#define LOGGER_H_191025

#include <QObject>

class Logger : public QObject {
  Q_OBJECT

public:
  Logger(QObject *parent = nullptr);
  virtual ~Logger();

signals:
  void message(const QString &msg);

private:
  static void messageHandler(QtMsgType type, const QMessageLogContext &context, const QString &msg);
};

#endif
