#ifndef ATOMICWIDGET_H_190506
#define ATOMICWIDGET_H_190506
/* What an AtomicForm remembers about its widgets */
#include <memory>
#include <QWidget>
#include "confKey.h"
#include "conf.h"

namespace conf {
  class Value;
};

/* We choose to have AtomicWidget a QObject, meaning the derived implementations
 * of an AtomicWidgets cannot inherit a QObject (ie any QWidget). Instead they
 * will have to have it as a member and redirect calls to the few interresting
 * QWidget functions to that member.
 * This allows us to use an AtomicWidget to indiscriminately manipulate any
 * value editor. */
class AtomicWidget : public QWidget
{
  Q_OBJECT

  bool last_enabled;

public:
  conf::Key const key;
  std::shared_ptr<conf::Value const> initValue; // shared ptr

  AtomicWidget(conf::Key const &key_, QWidget *parent = nullptr) :
    QWidget(parent),
    last_enabled(true),
    key(key_) {}

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

public slots:
  /* Return false if the editor can not display this value because of
   * incompatible types. */
  // TODO: replace the widget with an error message then.
  virtual bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>) = 0;

  void lockValue(conf::Key const &, QString const &uid)
  {
    setEnabled(uid == my_uid);
  }
  void unlockValue(conf::Key const &)
  {
    setEnabled(false);
  }

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>);
};

#endif
