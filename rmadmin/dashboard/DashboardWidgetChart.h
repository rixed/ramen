#ifndef DASHBOARDWIDGETCHART_H_200304
#define DASHBOARDWIDGETCHART_H_200304
#include <string>
#include "dashboard/DashboardWidget.h"

class QWidget;
class TimeChartEditor;
class TimeLineGroup;
struct TimeRange;

class DashboardWidgetChart : public DashboardWidget
{
  Q_OBJECT

  TimeChartEditor *chart;

public:
  DashboardWidgetChart(
    std::string const &key,
    TimeLineGroup *,
    QWidget *parent = nullptr);

protected:
  AtomicWidget *atomicWidget() const override;

public slots:
  void setTimeRange(TimeRange const &);

signals:
  void timeRangeChanged(TimeRange const &);
  void newTailTime(double);
};

#endif
