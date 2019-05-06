#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <iostream>
#include <QLabel>
#include "KWidget.h"
#include "confValue.h"

class KLabel : public QLabel, public KWidget
{
  Q_OBJECT

public:
  KLabel(std::string const key, QWidget *parent = nullptr) :
    QLabel(parent),
    KWidget(key)
  {}
  ~KLabel() {}

public slots:
  void setEnabled(bool enabled)
  {
    QLabel::setEnabled(enabled);
  }

  void setValue(conf::Value const &v)
  {
    std::cerr << "Setting KLabel" << std::endl;
    QLabel::setText(v.toQString());
  }

  void delValue(std::string const &)
  {
    // TODO: replace this widget with s tombstone?
  }
};

#endif
