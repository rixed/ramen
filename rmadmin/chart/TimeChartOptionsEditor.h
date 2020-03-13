#ifndef TIMECHARTOPTIONSEDITOR_H_200306
#define TIMECHARTOPTIONSEDITOR_H_200306
#include <memory>
#include <QWidget>
#include "confValue.h"

class QTabWidget;
class TimeChartEditWidget;
namespace conf {
  struct DashboardWidgetChart;
};

class TimeChartOptionsEditor : public QWidget
{
  Q_OBJECT

  TimeChartEditWidget *editWidget;

public:
  /* Each tab is a TimeChartAxisEditor */
  QTabWidget *axes;

  TimeChartOptionsEditor(TimeChartEditWidget *, QWidget *parent = nullptr);
  void setEnabled(bool);
  bool setValue(std::shared_ptr<conf::DashboardWidgetChart const>);

public slots:
  void updateAfterFieldChange(
    std::string const &site, std::string const &program,
    std::string const &function, std::string const &field);

signals:
  void axisChanged(int);
};

#endif
