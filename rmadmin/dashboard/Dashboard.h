#ifndef DASHBOARD_H_200304
#define DASHBOARD_H_200304
#include <list>
#include <string>
#include <QString>
#include <QWidget>

class DashboardWidget;
class KValue;
class QLabel;
class QVBoxLayout;
class TimeRangeEdit;
namespace conf {
  class DashboardWidget;
};

class Dashboard : public QWidget
{
  Q_OBJECT

  std::string const key_prefix;
  QString name;

  TimeRangeEdit *timeRangeEdit;

  struct WidgetRef {
    int idx;
    DashboardWidget *widget;

    WidgetRef(int idx_, DashboardWidget *widget_)
      : idx(idx_), widget(widget_) {}
  };
  std::list<WidgetRef> widgets; // ordered according to idx

  QVBoxLayout *vboxLayout;

  // The placeholder message when a dashboard is empty:
  QLabel *placeHolder;

public:
  Dashboard(std::string const key_prefix, QWidget *parent = nullptr);

protected:
  /* Add a widget, in the right order according to the key. The KValue must
   * be a conf::DashboardWidget. */
  void addWidget(std::string const &, KValue const &, int);
  void delWidget(int);

protected slots:
  void addValue(std::string const &, KValue const &);
  void delValue(std::string const &, KValue const &);
};
#endif
