#ifndef KLINEEDIT_H_190505
#define KLINEEDIT_H_190505
#include <QLineEdit>
#include "KValue.h"
#include "AtomicWidget.h"
#include "confValue.h"
#include "conf.h"

class KLineEdit : public AtomicWidget
{
  Q_OBJECT

  QLineEdit *lineEdit;

public:
  KLineEdit(conf::Key const &key, QWidget *parent = nullptr);

  void setPlaceholderText(QString const s) {
    lineEdit->setPlaceholderText(s);
  }

  virtual std::shared_ptr<conf::Value const> getValue() const;
  virtual void setEnabled(bool);

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>);

  void lockValue(conf::Key const &k, QString const &uid)
  {
    AtomicWidget::lockValue(k, uid);
  }

  void unlockValue(conf::Key const &k)
  {
    AtomicWidget::unlockValue(k);
  }

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
