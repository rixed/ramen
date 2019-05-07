#ifndef KVALUE_H_190506
#define KVALUE_H_190506
#include <string>
#include <optional>
#include <memory>
#include <QObject>
#include <QString>
#include "confValue.h"
#include "confKey.h"

class KValue : public QObject
{
  Q_OBJECT

  std::optional<QString> owner;
  std::shared_ptr<conf::Value const> val; // may not be set

public:
  KValue();
  KValue(const KValue&);
  ~KValue();
  void set(conf::Key const &, std::shared_ptr<conf::Value const>);
  std::shared_ptr<conf::Value const> value() const;
  void lock(conf::Key const &, QString const &);
  void unlock(conf::Key const &);
  KValue& operator=(const KValue&);
signals:
  void valueCreated(conf::Key const &, std::shared_ptr<conf::Value const>);
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>);
  void valueLocked(conf::Key const &, QString const &uid);
  void valueUnlocked(conf::Key const &);
  void valueDeleted(conf::Key const &);
};

#endif
