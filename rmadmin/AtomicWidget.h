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
  conf::Key key;
  std::shared_ptr<conf::Value const> initValue; // shared ptr

  AtomicWidget(QWidget *parent = nullptr) :
    QWidget(parent),
    last_enabled(true),
    key(conf::Key::null) {}

  /* As much as we'd like to build the widget and set its key in one go, we
   * cannot because virtual function extraConnections cannot be resolved at
   * construction time:
  AtomicWidget(conf::Key const &k, QWidget *parent) :
    AtomicWidget(parent) {
    setKey(k);
  } */

  bool isKeySet() const { return !(key == conf::Key::null); }

  virtual void setEnabled(bool enabled);

  // By default do not set any value (read-only):
  virtual std::shared_ptr<conf::Value const> getValue() const { return nullptr; }

protected:
  void setCentralWidget(QWidget *w);

  /* Called by setKey with the locked kvs and the kv of the new key, after
   * all former connections have been disconnected, so that implementers can
   * connect new signals if they are interested. */
  virtual void extraConnections(KValue *) {}

public slots:
  /* Return false if the editor can not display this value because of
   * incompatible types.
   * Note: need to share that value because AtomicWidget might keep a
   * copy. */
  // TODO: replace the widget with an error message then.
  virtual bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>) = 0;

  void setKey(conf::Key const &);

  void lockValue(conf::Key const &, QString const &uid)
  {
    setEnabled(uid == my_uid);
  }
  void unlockValue(conf::Key const &)
  {
    setEnabled(false);
  }

signals:
  void keyChanged(conf::Key const &old, conf::Key const &new_);
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>);
};

#endif
