#ifndef DASHBOARDWIDGETEDITOR_H_200320
#define DASHBOARDWIDGETEDITOR_H_200320
/* A small widget offering to move or copy a dashboard widget into another
 * dashboard, or to delete it. */
#include <string>
#include <QWidget>

class DashboardWidgetEditor : public QWidget
{
  Q_OBJECT

public:
  DashboardWidgetEditor(QWidget *parent = nullptr);
};

#endif
