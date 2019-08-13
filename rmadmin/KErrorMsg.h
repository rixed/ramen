#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include <QLabel>
#include "confValue.h"
#include "conf.h"

class KErrorMsg : public QLabel
{
  Q_OBJECT

  bool keyIsSet;

public:
  KErrorMsg(QWidget *parent = nullptr) : QLabel(parent), keyIsSet(false) {}

public slots:
  void setKey(conf::Key const &);

  void setValue(conf::Key const &, std::shared_ptr<conf::Value const>);
};

#endif
