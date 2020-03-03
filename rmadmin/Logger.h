#ifndef LOGGER_H_191025
#define LOGGER_H_191025
#include <QObject>
#include <QString>

QString const logLevel(QtMsgType);

class Logger : public QObject {
  Q_OBJECT

public:
  Logger(QObject *parent = nullptr);
  ~Logger();

signals:
  void newMessage(QtMsgType, QString const &);
};

Q_DECLARE_METATYPE(QtMsgType);

#endif
