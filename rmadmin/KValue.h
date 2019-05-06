#ifndef KVALUE_H_190506
#define KVALUE_H_190506
#include <string>
#include <optional>
#include <QObject>
#include <QString>
#include "confValue.h"
#include "confKey.h"

class KValue : public QObject
{
  Q_OBJECT

  std::optional<QString> owner;
  conf::Value val; // may not be initialized

public:
  KValue();
  KValue(const KValue&);
  ~KValue();
  void set(conf::Key const &, conf::Value const &);
  void lock(conf::Key const &, QString const &);
  void unlock(conf::Key const &);
  KValue& operator=(const KValue&);
signals:
  void valueCreated(conf::Key const &key, conf::Value const &v);
  void valueChanged(conf::Key const &key, conf::Value const &v);
  void valueLocked(conf::Key const &key, QString const &uid);
  void valueUnlocked(conf::Key const &key);
  void valueDeleted(conf::Key const &key);
};

#endif
