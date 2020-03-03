#ifndef TIMECHARTEDITWIDGET_H_200306
#define TIMECHARTEDITWIDGET_H_200306
#include "AtomicWidget.h"

class TimeChartEditWidget : public AtomicWidget
{
  Q_OBJECT

public:
  TimeChartEditWidget(QWidget *parent = nullptr);
  void setEnabled(bool);

public slots:
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);
};

#endif
