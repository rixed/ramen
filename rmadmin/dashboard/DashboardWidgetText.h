#ifndef DASHBOARDWIDGETTEXT_H_200304
#define DASHBOARDWIDGETTEXT_H_200304
#include <memory>
#include "dashboard/DashboardWidget.h"

class QLabel;
class QWidget;

class DashboardWidgetText : public DashboardWidget
{
  Q_OBJECT

  /* TODO: Have an actual DashboardTitleEditor editor that's an
   * AtomicForm etc. */
  QLabel *text;

public:
  DashboardWidgetText(std::string const &key, QWidget *parent = nullptr);
};

#endif
