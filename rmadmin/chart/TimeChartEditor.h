#ifndef TIMECHARTEDITOR_H_200306
#define TIMECHARTEDITOR_H_200306
#include <QWidget>

class QPushButton;
class QWidget;
class TimeChart;
class TimeChartEditWidget;
class TimeLine;
class TimeLineGroup;
struct TimeRange;

class TimeChartEditor : public QWidget
{
  Q_OBJECT

  TimeLine *timeLine;

public:
  TimeChartEditWidget *editWidget;
  TimeChart *chart;
  QWidget *timeLines;

  TimeChartEditor(
    QPushButton *submitButton,
    QPushButton *cancelButton,
    TimeLineGroup *timeLineGroup,
    QWidget *parent = nullptr);

  ~TimeChartEditor();

protected:
  void resizeEvent(QResizeEvent *);

signals:
  void timeRangeChanged(TimeRange const &);
  void newTailTime(double);
};

#endif
