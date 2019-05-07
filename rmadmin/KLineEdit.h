#ifndef KLINEEDIT_H_190505
#define KLINEEDIT_H_190505
#include <QLineEdit>
#include "KValue.h"
#include "AtomicWidget.h"
#include "confValue.h"
#include "conf.h"

class KLineEdit : public QLineEdit, public AtomicWidget
{
  Q_OBJECT
  //Q_INTERFACES(AtomicWidget)

  conf::ValueType valueType;

public:
  KLineEdit(std::string const key, conf::ValueType valueType_, QWidget *parent = nullptr) :
    QLineEdit(parent),
    AtomicWidget(key),
    valueType(valueType_)
  {
    KValue &kv = conf::kvs[key];
    QObject::connect(&kv, &KValue::valueCreated, this, &KLineEdit::setValue);
    QObject::connect(&kv, &KValue::valueChanged, this, &KLineEdit::setValue);
    QObject::connect(&kv, &KValue::valueLocked, this, &KLineEdit::lockValue);
    QObject::connect(&kv, &KValue::valueUnlocked, this, &KLineEdit::unlockValue);
  }
  ~KLineEdit() {}

  virtual std::shared_ptr<conf::Value const> getValue() const
  {
    return std::shared_ptr<conf::Value const>(conf::valueOfQString(valueType, text()));
  }

  void setEnabled(bool enabled)
  {
    AtomicWidget::setEnabled(enabled);
    QLineEdit::setEnabled(enabled);
  }

public slots:
  void setValue(conf::Key const &, std::shared_ptr<conf::Value const> v)
  {
    QLineEdit::setText(v->toQString());
  }

  void lockValue(conf::Key const &, QString const &uid)
  {
    KLineEdit::setEnabled(uid == conf::my_uid);
  }

  void unlockValue(conf::Key const &)
  {
    KLineEdit::setEnabled(false);
  }
};

#endif
