#ifndef ATOMICWIDGET_H_190506
#define ATOMICWIDGET_H_190506
/* What an AtomicForm remembers about its widgets */
#include <memory>
#include <QWidget>
#include "conf.h"

struct KValue;
class QStackedLayout;
namespace conf {
  class Value;
};

/* We choose to have AtomicWidget a QObject, meaning the derived implementations
 * of an AtomicWidgets cannot also inherit a QObject (ie any QWidget). Instead they
 * will have to include this QWidget they wish they inherited from as a member
 * and redirect calls to the few interesting QWidget functions to that member.
 * To help with this (esp. sizing) pass your widget to relayoutWidget.
 * This allows us to use an AtomicWidget to indiscriminately manipulate any
 * value editor. */
class AtomicWidget : public QWidget
{
  Q_OBJECT

  QStackedLayout *layout;

  // For the default implementation of setKey()/key()
  std::string _key;

  void setValueFromStore(std::string const &, KValue const &);

  void lockValue(std::string const &, KValue const &);
  void unlockValue(std::string const &, KValue const &);
  void forgetValue(std::string const &, KValue const &);

public:
  AtomicWidget(QWidget *parent = nullptr);

  /* As much as we'd like to build the widget and set its key in one go, we
   * cannot because virtual function setValues cannot be resolved at
   * construction time:
  AtomicWidget(std::string const &k, QWidget *parent) :
    AtomicWidget(parent) {
    setKey(k);
  } */

  virtual std::string const &key() const { return _key; }

  // Called by setKey to actually save that new key:
  virtual void saveKey(std::string const &newKey) { _key = newKey; }

  virtual void setEnabled(bool) = 0;

  // By default do not set any value (read-only):
  virtual std::shared_ptr<conf::Value const> getValue() const { return nullptr; }

  /* Return false if the editor can not display this value because of
   * incompatible types.
   * Note: need to share that value because AtomicWidget might keep a
   * copy. */
  // TODO: replace the widget with an error message then.
  virtual bool setValue(
    std::string const &, std::shared_ptr<conf::Value const>) = 0;

  virtual bool hasValidInput() const { return true; }

  /* We want the AtomicWidget to survive the removal of the key from the
   * kvs so we merely take and store the key name and will lookup the kvs
   * each time we need the actual value (which is almost never - the widget
   * produces the value).
   * Returns whether the value was accepted by setValue. */
  virtual bool setKey(std::string const &);

protected:
  void relayoutWidget(QWidget *w);

public slots:
  void onChange(QList<ConfChange> const &);

signals:
  // Triggered when the key is changed:
  void keyChanged(std::string const &old, std::string const &new_);
  // Triggered when the underlying value is changed
  void valueChanged(std::string const &, std::shared_ptr<conf::Value const>);
  // Triggered when the edited value is changed
  void inputChanged();
};

#endif
