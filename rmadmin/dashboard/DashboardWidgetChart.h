#ifndef DASHBOARDWIDGETCHART_H_200304
#define DASHBOARDWIDGETCHART_H_200304
#include <string>
#include "AtomicWidget.h"

class DashboardWidgetForm;
class QWidget;
class TimeChartEditor;
class TimeLineGroup;
struct TimeRange;

class DashboardWidgetChart : public AtomicWidget
{
  Q_OBJECT

  TimeChartEditor *chart;

public:
  DashboardWidgetChart(
    // Passed to steal its form buttons:
    DashboardWidgetForm *,
    QWidget *parent = nullptr);

  void setEnabled(bool);
  std::shared_ptr<conf::Value const> getValue() const;
  bool setValue(std::string const &, std::shared_ptr<conf::Value const>);

public slots:
  void setTimeRange(TimeRange const &);

signals:
  void timeRangeChanged(TimeRange const &);
  void newTailTime(double);
};

#endif
