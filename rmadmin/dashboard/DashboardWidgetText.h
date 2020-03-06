#ifndef DASHBOARDWIDGETTEXT_H_200304
#define DASHBOARDWIDGETTEXT_H_200304
#include <string>
#include "dashboard/DashboardWidget.h"

class DashboardTextEditor;
class QWidget;

class DashboardWidgetText : public DashboardWidget
{
  Q_OBJECT

  DashboardTextEditor *editor;

public:
  DashboardWidgetText(std::string const &key, QWidget *parent = nullptr);
};

#endif
