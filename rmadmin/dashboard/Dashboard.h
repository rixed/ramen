#ifndef DASHBOARD_H_200304
#define DASHBOARD_H_200304
#include <list>
#include <string>
#include <QString>
#include <QWidget>
#include "conf.h"

class DashboardWidgetForm;
class FunctionSelector;
struct KValue;
class QLabel;
class QSplitter;
class TimeLineGroup;
class TimeRangeEdit;
namespace conf {
  struct DashboardWidget;
  class Value;
};

class Dashboard : public QWidget
{
  Q_OBJECT

  std::string const keyPrefix;
  QString name;

  struct WidgetRef {
    int idx;
    DashboardWidgetForm *widget;

    WidgetRef(int idx_, DashboardWidgetForm *widget_)
      : idx(idx_), widget(widget_) {}
  };
  std::list<WidgetRef> widgets; // ordered according to idx

  QSplitter *splitter;

  // The placeholder message when a dashboard is empty:
  QLabel *placeHolder;

  bool isMyKey(std::string const &);
  void resetArrows();

  void addValue(std::string const &, KValue const &);
  void delValue(std::string const &, KValue const &);

public:
  TimeLineGroup *timeLineGroup;
  FunctionSelector *functionSelector;
  TimeRangeEdit *timeRangeEdit;

  // keyPrefix: up to but not including "/widgets"
  Dashboard(std::string const keyPrefix, QWidget *parent = nullptr);

protected:
  /* Add a widget, in the right order according to the key. The KValue must
   * be a conf::DashWidget. */
  void addWidget(std::string const &, int);

  void delWidget(int);

public slots:
  void setTailTime(double);

protected slots:
  void onChange(QList<ConfChange> const &);
  void addCurrentFunction();
};
#endif
