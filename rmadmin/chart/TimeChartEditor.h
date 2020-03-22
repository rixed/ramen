#ifndef TIMECHARTEDITOR_H_200306
#define TIMECHARTEDITOR_H_200306
#include <QWidget>

class QPushButton;
class QWidget;
class TimeChart;
class TimeChartEditWidget;
class TimeLineGroup;
struct TimeRange;

class TimeChartEditor : public QWidget
{
  Q_OBJECT

public:
  TimeChartEditWidget *editWidget;
  TimeLineGroup *timeLineGroup;
  TimeChart *chart;
  QWidget *timeLines;

  TimeChartEditor(
    QPushButton *submitButton,
    QPushButton *cancelButton,
    QWidget *parent = nullptr);

signals:
  void timeRangeChanged(TimeRange const &);
  void newTailTime(double);
};

#endif
