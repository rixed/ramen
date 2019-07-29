#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <iostream>
#include <cassert>
#include <QLabel>
#include "AtomicWidget.h"

class KLabel : public AtomicWidget
{
  Q_OBJECT

  QLabel *label;

public:
  KLabel(conf::Key const &key, QWidget *parent = nullptr, bool wordWrap = false);

  void setEnabled(bool) {} // not editable

public slots:
  bool setValue(conf::Key const &, std::shared_ptr<conf::Value const>);

signals:
  void valueChanged(conf::Key const &, std::shared_ptr<conf::Value const>) const;
};

#endif
