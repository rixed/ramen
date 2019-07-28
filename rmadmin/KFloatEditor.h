#ifndef KFLOATEDITOR_H_190727
#define KFLOATEDITOR_H_190727
#include <QLineEdit>
#include "confValue.h"
#include "AtomicWidget.h"

struct KFloatEditor : public AtomicWidget
{
  Q_OBJECT

  QLineEdit *lineEdit;

public:
  KFloatEditor(conf::Key const &key,
               QWidget *parent = nullptr,
               double min = -std::numeric_limits<double>::infinity(),
               double max = std::numeric_limits<double>::infinity());

   std::shared_ptr<conf::Value const> getValue() const;
   void setEnabled(bool);

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
