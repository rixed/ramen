#ifndef TIMECHARTCOLUMNEDITOR_H_200307
#define TIMECHARTCOLUMNEDITOR_H_200307
#include <QWidget>
#include "confValue.h"

class QLabel;
class QLineEdit;
class QPushButton;

class TimeChartColumnEditor : public QWidget
{
  Q_OBJECT

  QPushButton *representation; // FIXME: need to know the tot number of S/SC columns
  QLineEdit *opacity;
  QLineEdit *color;
  QLineEdit *factors; // FIXME: need to know the possible factors
  QPushButton *axis; // FIXME: need to know the tot number of L and R axis

public:
  QLabel *name;
  TimeChartColumnEditor(QWidget *parent = nullptr);
  void setEnabled(bool);
  bool setValue(conf::DashboardWidgetChart::Column const &);
};

#endif
