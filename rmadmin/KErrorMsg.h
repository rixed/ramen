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
    KValue &kv = conf::kvs[key];
    QObject::connect(&kv, &KValue::valueChanged, this, &KErrorMsg::setValue);
  }
  ~KErrorMsg() {}

public slots:
  void setValue(conf::Key const &, conf::Value const &v)
  {
    QString s(v.toQString());
    setStyleSheet(
      s.length() == 0 ?
        "" :
        "background-color: pink");
    QLabel::setText(s);
  }
};

#endif
