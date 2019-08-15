#ifndef ATOMICWIDGET_H_190506
#define ATOMICWIDGET_H_190506
/* What an AtomicForm remembers about its widgets */
#include <memory>
#include <QWidget>
#include "KVPair.h"

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
  std::string key;
  std::shared_ptr<conf::Value const> initValue; // shared ptr

  AtomicWidget(QWidget *parent = nullptr);

  /* As much as we'd like to build the widget and set its key in one go, we
   * cannot because virtual function setValues cannot be resolved at
   * construction time:
  AtomicWidget(std::string const &k, QWidget *parent) :
    AtomicWidget(parent) {
    setKey(k);
  } */

  virtual void setEnabled(bool enabled);

  // By default do not set any value (read-only):
  virtual std::shared_ptr<conf::Value const> getValue() const { return nullptr; }

  virtual bool setValue(std::string const &, std::shared_ptr<conf::Value const>) = 0;

protected:
  void relayoutWidget(QWidget *w);

public slots:
  /* Return false if the editor can not display this value because of
   * incompatible types.
   * Note: need to share that value because AtomicWidget might keep a
   * copy. */
  // TODO: replace the widget with an error message then.
  void setValueFromStore(KVPair const &);

  /* We want the AtomicWidget to survive the removal of the key from the
   * kvs so we merely take and store the key name and will lookup the kvs
   * each time we need the actual value (which is almost never - the widget
   * produces the value): */
  void setKey(std::string const &);

  void lockValue(KVPair const &);
  void unlockValue(KVPair const &);
  void forgetValue(KVPair const &);

signals:
  // Triggered when the key is changed:
  void keyChanged(std::string const &old, std::string const &new_);
  // Triggered when the underlying value is changed
  void valueChanged(std::string const &, std::shared_ptr<conf::Value const>);
  // Triggered when the edited value is changed
  void inputChanged();
};

#endif
