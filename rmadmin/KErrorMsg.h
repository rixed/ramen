#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <iostream>
#include <optional>
#include <QLabel>
#include "confValue.h"
#include "conf.h"

class KErrorMsg : public QLabel
{
  Q_OBJECT

  bool keyIsSet;

public:
  KErrorMsg(QWidget *parent = nullptr) : QLabel(parent), keyIsSet(false) {}

public slots:
  void setKey(conf::Key const key)
  {
    assert(! keyIsSet);
    keyIsSet = true;
    std::cout << "KErrorMsg: setting key to " << key << std::endl;
    conf::kvs_lock.lock_shared();
    KValue &kv = conf::kvs[key];
    conf::kvs_lock.unlock_shared();
    connect(&kv, &KValue::valueChanged, this, &KErrorMsg::setValue);
    if (kv.isSet()) setValue(key, kv.val);
  }

  void setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
  {
    std::shared_ptr<conf::Error const> err =
      std::dynamic_pointer_cast<conf::Error const>(v);
    assert(err);
    QLabel::setStyleSheet(
      err->msg.length() == 0 ?
        "" :
        "background-color: pink");
    QLabel::setText(QString::fromStdString(err->msg));
  }
};

#endif
