#ifndef KVALUE_H_190506
#define KVALUE_H_190506
#include <string>
#include <optional>
#include <QObject>
#include "confValue.h"

class KValue : QObject
{
  Q_OBJECT

  std::string const key;
  std::optional<std::string const> owner;
  conf::Value value; // may not be initialized

public:
  KValue(std::string const &);
  ~KValue();
  void set(conf::Value const &);
  void lock(std::string const &);
  void unlock();

signals:
  void valueCreated(std::string const &key);
  void valueChanged(std::string const &key);
  void valueLocked(std::string const &key, std::string const &uid);
  void valueUnlocked(std::string const &key);
  void valueDeleted(std::string const &key);
};

#endif
