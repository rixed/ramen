#ifndef KFLOATEDITOR_H_190727
#define KFLOATEDITOR_H_190727
#include <QLineEdit>
#include "confValue.h"
#include "AtomicWidget.h"

struct KFloatEditor : public QLineEdit, public AtomicWidget
{
  Q_OBJECT

public:
  KFloatEditor(std::string const &key, double min = 0., double max = 1., QWidget *parent = nullptr);

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
