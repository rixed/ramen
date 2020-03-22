#ifndef DASHBOARDWIDGETCHART_H_200304
#define DASHBOARDWIDGETCHART_H_200304
#include "dashboard/DashboardWidget.h"

class QWidget;
class TimeChartEditor;
struct TimeRange;

class DashboardWidgetChart : public DashboardWidget
{
  Q_OBJECT

  TimeChartEditor *chart;

public:
  DashboardWidgetChart(std::string const &key, QWidget *parent = nullptr);

protected:
  AtomicWidget *atomicWidget() const override;

public slots:
  void setTimeRange(TimeRange const &);

signals:
  void timeRangeChanged(TimeRange const &);
  void newTailTime(double);
};

#endif
