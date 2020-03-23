#ifndef DASHBOARDWIDGETTEXT_H_200304
#define DASHBOARDWIDGETTEXT_H_200304
#include <string>
#include "dashboard/DashboardWidget.h"

class DashboardTextEditor;
class QWidget;
class QPushButton;

class DashboardWidgetText : public DashboardWidget
{
  Q_OBJECT

  DashboardTextEditor *editor;
  QWidget *widget;

public:
  DashboardWidgetText(
    std::string const &key,
    QWidget *parent = nullptr);

protected:
  AtomicWidget *atomicWidget() const override;
};

#endif
