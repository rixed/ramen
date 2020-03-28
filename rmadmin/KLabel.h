#ifndef KLABEL_H_190505
#define KLABEL_H_190505
#include "AtomicWidget.h"

class QLabel;

class KLabel : public AtomicWidget
{
  Q_OBJECT

  QLabel *label;


public:
  KLabel(QWidget *parent = nullptr, bool wordWrap = false);

  void setEnabled(bool) {}

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);
};

#endif
