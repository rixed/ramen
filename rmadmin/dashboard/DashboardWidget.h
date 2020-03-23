#ifndef DASHBOARDWIDGET_H_200304
#define DASHBOARDWIDGET_H_200304
#include <memory>
#include "AtomicForm.h"

class DashboardCopyDialog;
class QAction;
class QVBoxLayout;
class QWidget;
namespace conf {
  class Value;
};

/* This is an AtomicForm that's composed of a single AtomicWidget,
 * since all widget values are madde of a single KValue. */
class DashboardWidget : public AtomicForm
{
  Q_OBJECT

  std::string widgetKey;

  QVBoxLayout *layout;
  DashboardCopyDialog *copyDialog;
  QWidget *menuFrame;

  QAction *upAction, *downAction;

  void doCopy(bool);
  void switchPosition(std::string const &, KValue const &);

public:
  DashboardWidget(
    std::string const &widgetKey,
    QWidget *parent = nullptr);

  void enableArrowsForPosition(size_t idx, size_t count);

protected:
  virtual AtomicWidget *atomicWidget() const = 0;

  void setCentralWidget(QWidget *);

protected slots:
  void performCopy();
  void performMove();
  void moveUp();
  void moveDown();
};

#endif
