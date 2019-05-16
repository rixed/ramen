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

  conf::ValueType valueType;

public:
  KLineEdit(std::string const key, conf::ValueType, QWidget *parent = nullptr);

  virtual std::shared_ptr<conf::Value const> getValue() const;
  void setEnabled(bool);

public slots:
  void setValue(conf::Key const &, std::shared_ptr<conf::Value const> v);
  void lockValue(conf::Key const &k, QString const &uid)
  {
    AtomicWidget::lockValue(k, uid);
  }
  void unlockValue(conf::Key const &k)
  {
    AtomicWidget::unlockValue(k);
  }
};

#endif
