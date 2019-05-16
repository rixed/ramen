#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <iostream>
#include <QLabel>
#include "confValue.h"
#include "conf.h"

class KErrorMsg : public QLabel
{
  Q_OBJECT

public:
  KErrorMsg(conf::Key const key, QWidget *parent = nullptr) :
    QLabel(parent)
  {
    conf::kvs_lock.lock_shared();
    KValue &kv = conf::kvs[key];
    conf::kvs_lock.unlock_shared();
    QObject::connect(&kv, &KValue::valueChanged, this, &KErrorMsg::setValue);
    if (kv.isSet()) setValue(key, kv.value());
  }
  ~KErrorMsg() {}

public slots:
  void setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
  {
    QString s(v->toQString());
    setStyleSheet(
      s.length() == 0 ?
        "" :
        "background-color: pink");
    QLabel::setText(s);
  }
};

#endif
