#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <iostream>
#include <QLabel>
#include "confValue.h"
#include "conf.h"
#include "AtomicWidget.h"

class KLabel : public QLabel, public AtomicWidget
{
  Q_OBJECT

public:
  KLabel(conf::Key const key, QWidget *parent = nullptr) :
    QLabel(parent), AtomicWidget(key)
  {
    conf::kvs_lock.lock_shared();
    KValue &kv = conf::kvs[key];
    conf::kvs_lock.unlock_shared();
    connect(&kv, &KValue::valueChanged, this, &KLabel::setValue);

    if (kv.isSet()) setValue(key, kv.val);
  }

public slots:
  void setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
  {
    QString new_v(v->toQString());
    if (new_v != text()) {
      QLabel::setText(new_v);
      emit valueChanged(k, v);
    }
  }

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
