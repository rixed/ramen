#ifndef ATOMICWIDGET_H_190506
#define ATOMICWIDGET_H_190506
/* What an AtomicForm remembers about its widgets */
#include <QWidget>
#include <memory>
#include "confKey.h"
#include "conf.h"

namespace conf {
  class Value;
};

class AtomicWidget
{
  bool last_enabled;

public:
  conf::Key const key;
  std::shared_ptr<conf::Value const> initValue; // shared ptr

  AtomicWidget(conf::Key const &key_) :
    last_enabled(true),
    key(key_)
  {}

  virtual ~AtomicWidget() {}

  virtual void setEnabled(bool enabled)
  {
    if (enabled && !last_enabled) {
      // Capture the value at the beginning of edition:
     initValue = getValue();
    }
    last_enabled = enabled;
  }

  // By default do not set any value (read-only):
  virtual std::shared_ptr<conf::Value const> getValue() const { return nullptr; }

  virtual void setValue(conf::Key const &, std::shared_ptr<conf::Value const>) = 0;

  /* AtomicWidget not being a QObject, we won't be able to connect to virtual
   * slots so no need to declare those virtual: */
  void lockValue(conf::Key const &, QString const &uid)
  {
    setEnabled(uid == conf::my_uid);
  }
  void unlockValue(conf::Key const &)
  {
    setEnabled(false);
  }

signals:
  /* AtomicWidget is not (and cannot be) a Q_Object but we want all of its
   * descendants to have this signal: */
  virtual void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const = 0;
};

#endif
