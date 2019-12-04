#ifndef LOGGER_H_191025
#define LOGGER_H_191025
#include <QObject>

class QString;

class Logger : public QObject {
  Q_OBJECT

public:
  Logger(QObject *parent = nullptr);
  ~Logger();

signals:
  void newMessage(const QString &);
};

#endif
