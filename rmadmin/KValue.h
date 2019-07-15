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
  // TODO: bring the set_by and mtime down there

public:
  KValue();
  KValue(const KValue&);
  ~KValue();
  void set(conf::Key const &, std::shared_ptr<conf::Value const>);
  bool isSet() const {
    return val != nullptr;
  }
  bool isLocked() const {
    return isSet() && owner != nullptr;
  }
  std::shared_ptr<conf::Value const> value() const;
  void lock(conf::Key const &, QString const &);
  void unlock(conf::Key const &);
  KValue& operator=(const KValue&);

signals:
  void valueCreated(conf::Key const &, std::shared_ptr<conf::Value const>) const;
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
  void valueLocked(conf::Key const &, QString const &uid) const;
  void valueUnlocked(conf::Key const &) const;
  void valueDeleted(conf::Key const &) const;
};

#endif
