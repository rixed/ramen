#ifndef DASHBOARDWIDGET_H_200304
#define DASHBOARDWIDGET_H_200304
#include <memory>
#include "AtomicWidget.h"

class Dashboard;
class DashboardCopyDialog;
class DashboardWidgetChart;
class DashboardWidgetForm;
class DashboardWidgetText;
class QAction;
class QStackedLayout;
class QWidget;
namespace conf {
  class Value;
};

/* This is an AtomicWidget that can switch representation between a chart
 * or a text box, thus surviving setKey that change the widget type.
 * It is the AtomicWidget that is added in the DashboardWidgetForm; the
 * underlying DashboardWidgetText/Chart is not. */
class DashboardWidget : public AtomicWidget
{
  Q_OBJECT

  std::string widgetKey;
  Dashboard *dashboard;
  DashboardWidgetForm *widgetForm;

  // Either none, one or the other is ever instantiated.
  DashboardWidgetText *widgetText;
  DashboardWidgetChart *widgetChart;
  AtomicWidget *current;

  QString title;

public:
  /* If this is part of a dashboard (tu reuse its time setting) then
   * pass it, else nullptr: */
  DashboardWidget(
    Dashboard *, DashboardWidgetForm *, QWidget *parent = nullptr);

  void setEnabled(bool);
  std::shared_ptr<conf::Value const> getValue() const;
  bool setValue(
    std::string const &, std::shared_ptr<conf::Value const>);

signals:
  void titleChanged(QString const &);
};

#endif
