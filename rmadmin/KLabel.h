#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <iostream>
#include <cassert>
#include <QLabel>
#include "confValue.h"
#include "conf.h"
#include "AtomicWidget.h"

class KLabel : public AtomicWidget
{
  Q_OBJECT

  QLabel *label;

public:
  KLabel(conf::Key const key, bool wordWrap = false, QWidget *parent = nullptr);

  void setEnabled(bool) {} // not editable

public slots:
  bool setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
  {
    QString new_v(v->toQString());
    if (new_v != label->text()) {
      label->setText(new_v);
      emit valueChanged(k, v);
    }

    return true;
  }

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
