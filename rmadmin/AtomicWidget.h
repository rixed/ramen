#ifndef ATOMICWIDGET_H_190506
#define ATOMICWIDGET_H_190506
/* What an AtomicForm remembers about its widgets */
#include <QWidget>
#include "confKey.h"
#include "confValue.h"

struct AtomicWidget
{
  conf::Key const key;
  conf::Value initValue;

  AtomicWidget(conf::Key const &key_, conf::Value const &initValue_) :
    key(key_), initValue(initValue_)
  {
  }

  ~AtomicWidget() {}

  bool edited() const { return false; /* TODO */ }
  void resetValue() { /* TODO */ }
};

#endif
