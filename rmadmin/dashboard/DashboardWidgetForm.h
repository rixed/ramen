#ifndef DASHBOARDWIDGETFORM_H_200323
#define DASHBOARDWIDGETFORM_H_200323
#include <string>
#include "AtomicForm.h"

class Dashboard;
class DashboardCopyDialog;
class DashboardWidget;
class QAction;
class QLabel;
class QVBoxLayout;
class QWidget;
namespace conf {
  class Value;
};

/* This is an AtomicForm that's composed of a single AtomicWidget,
 * since all widget values are madde of a single KValue. */
class DashboardWidgetForm : public AtomicForm
{
  Q_OBJECT

  std::string widgetKey;
  DashboardWidget *widget;

  QVBoxLayout *layout;
  DashboardCopyDialog *copyDialog;
  QWidget *menuFrame;
  QLabel *title;

  QAction *upAction, *downAction;

  void doCopy(bool);
  void switchPosition(std::string const &, KValue const &);

public:
  Dashboard *dashboard;

  DashboardWidgetForm(
    std::string const &widgetKey,
    Dashboard *,
    QWidget *parent = nullptr);

  ~DashboardWidgetForm();

  void enableArrowsForPosition(size_t idx, size_t count);

  void setExpand(bool);

protected slots:
  void performCopy();
  void performMove();
  void moveUp();
  void moveDown();
  void setTitle(QString const &);
};

#endif
