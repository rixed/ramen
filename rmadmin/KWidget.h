#ifndef ATOMICWIDGET_H_190504
#define ATOMICWIDGET_H_190504
#include <string>
#include <optional>
#include <QObject>
#include "confValue.h"
#include "conf.h"

class KWidget
{
  /* We keep as much info as possible within the KWidget so that it's
   * easier to use it independently of any AtomicForm. */
  // As keys are rather short values are merely copied here:
  std::optional<conf::Value> serverValue;  // last sync received value

public:
  std::optional<std::string const> key;
  KWidget(std::string const key_); // Register for that key
  KWidget() {} // does not register anything
  virtual ~KWidget();

  virtual void setEnabled(bool) = 0;

  virtual void setValue(conf::Value const &) = 0;

  virtual void delValue(std::string const &) = 0;

  virtual void lockValue(std::string const &, std::string const &uid)
  {
    setEnabled(uid == conf::my_uid);
  }

  virtual void unlockValue(std::string const &)
  {
    setEnabled(false);
  }
};

#endif
