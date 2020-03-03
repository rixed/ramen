#ifndef DASHBOARDWIDGETCHART_H_200304
#define DASHBOARDWIDGETCHART_H_200304
#include <memory>
#include "dashboard/DashboardWidget.h"

class QWidget;
class TimeChartEditor;

class DashboardWidgetChart : public DashboardWidget
{
  Q_OBJECT

  TimeChartEditor *chart;

public:
  DashboardWidgetChart(std::string const &key, QWidget *parent = nullptr);
};

#endif
