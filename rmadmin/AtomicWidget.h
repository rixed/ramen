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
 * will have to have it as a member and redirect calls to the few interesting
 * QWidget functions to that member.
 * To help with this (esp. sizing) pass your widget to setCentralWidget.
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

  /* We'd like the above constructor to be able to set its initial value
   * but that needs to call the viortual setValue, so macros to the
   * rescue: */
# define SET_INITIAL_VALUE \
    conf::kvs_lock.lock_shared(); \
    KValue &kv = conf::kvs[key]; \
    if (kv.isSet()) { \
      bool ok = setValue(key, kv.val); \
      assert(ok); /* ? */ \
    } \
    setEnabled(kv.isMine()); \
    conf::kvs_lock.unlock_shared();

  virtual void setEnabled(bool enabled);

  // By default do not set any value (read-only):
  virtual std::shared_ptr<conf::Value const> getValue() const { return nullptr; }

protected:
  void setCentralWidget(QWidget *w);

public slots:
  /* Return false if the editor can not display this value because of
   * incompatible types.
   * Note: need to share that value because AtomicWidget might keep a
   * copy. */
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
