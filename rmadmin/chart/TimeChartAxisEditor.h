#ifndef TIMECHARTAXISEDITOR_H_200309
#define TIMECHARTAXISEDITOR_H_200309
#include <QWidget>
#include "confValue.h"

class QCheckBox;
class QRadioButton;

class TimeChartAxisEditor : public QWidget
{
  Q_OBJECT

public:
  QRadioButton *left, *right;
  QCheckBox *forceZero;
  QRadioButton *linear, *logarithmic;

  TimeChartAxisEditor(QWidget *parent = nullptr);

  bool setValue(conf::DashWidgetChart::Axis const &);
  conf::DashWidgetChart::Axis getValue() const;

signals:
  void valueChanged();
};

#endif
