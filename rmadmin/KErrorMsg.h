#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <iostream>
#include <QLabel>
#include "KWidget.h"
#include "confValue.h"

class KErrorMsg : public QLabel, public KWidget
{
  Q_OBJECT

public:
  KErrorMsg(std::string const key, QWidget *parent = nullptr) :
    QLabel(parent),
    KWidget(key)
  {}
  ~KErrorMsg() {}

public slots:
  // We do not care about locks for error messages:
  void setEnabled(bool) {}

  void setValue(conf::Value const &v)
  {
    QString s(v.toQString());
    setStyleSheet(
      s.length() == 0 ?
        "" :
        "background-color: pink");
    QLabel::setText(s);
  }

  void delValue(std::string const &)
  {
    // TODO: replace this widget with s tombstone?
  }
};

#endif
