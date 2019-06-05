#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <iostream>
#include <QLabel>
#include "confValue.h"
#include "conf.h"
#include "AtomicWidget.h"

class KErrorMsg : public QLabel, public AtomicWidget
{
  Q_OBJECT

  std::shared_ptr<conf::Value const> value;

public:
  KErrorMsg(conf::Key const key, QWidget *parent = nullptr) :
    QLabel(parent), AtomicWidget(key)
  {
    conf::kvs_lock.lock_shared();
    KValue &kv = conf::kvs[key];
    conf::kvs_lock.unlock_shared();
    connect(&kv, &KValue::valueChanged, this, &KErrorMsg::setValue);
    if (kv.isSet()) setValue(key, kv.value());
  }

public slots:
  void setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
  {
    value = v;
    QString s(v->toQString());
    QLabel::setStyleSheet(
      s.length() == 0 ?
        "" :
        "background-color: pink");
    QLabel::setText(s);
  }

  std::shared_ptr<conf::Value const> getValue() const { return value; }
};

#endif
