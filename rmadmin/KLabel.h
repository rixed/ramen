#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <iostream>
#include <QLabel>
#include "confValue.h"
#include "conf.h"

class KLabel : public QLabel
{
  Q_OBJECT

public:
  KLabel(conf::Key const key, QWidget *parent = nullptr) :
    QLabel(parent)
  {
    KValue &kv = conf::kvs[key];
    QObject::connect(&kv, &KValue::valueChanged, this, &KLabel::setValue);
  }
  ~KLabel() {}

public slots:
  void setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
  {
    QLabel::setText(v->toQString());
  }
};

#endif
