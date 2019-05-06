#ifndef KLINEEDIT_H_190505
#define KLINEEDIT_H_190505
#include <QLineEdit>
#include "KValue.h"
#include "confValue.h"
#include "conf.h"

class KLineEdit : public QLineEdit
{
  Q_OBJECT

public:
  KLineEdit(std::string const key, QWidget *parent = nullptr) :
    QLineEdit(parent)
  {
    KValue &kv = conf::kvs[key];
    QObject::connect(&kv, &KValue::valueCreated, this, &KLineEdit::setValue);
    QObject::connect(&kv, &KValue::valueChanged, this, &KLineEdit::setValue);
    QObject::connect(&kv, &KValue::valueLocked, this, &KLineEdit::lockValue);
    QObject::connect(&kv, &KValue::valueUnlocked, this, &KLineEdit::unlockValue);
  }
  ~KLineEdit() {}

public slots:
  void setValue(conf::Key const &, conf::Value const &v)
  {
    QLineEdit::setText(v.toQString());
  }

  void lockValue(conf::Key const &, QString const &uid)
  {
    setEnabled(uid == conf::my_uid);
  }

  void unlockValue(conf::Key const &)
  {
    setEnabled(false);
  }

};

#endif
