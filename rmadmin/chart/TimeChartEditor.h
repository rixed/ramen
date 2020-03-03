#ifndef TIMECHARTEDITOR_H_200306
#define TIMECHARTEDITOR_H_200306
#include <memory>
#include <string>
#include <QWidget>

class QWidget;
class TimeChart;
class TimeChartEditForm;

class TimeChartEditor : public QWidget
{
  Q_OBJECT

  TimeChartEditForm *editPanel;
  TimeChart *chart;

public:
  TimeChartEditor(
    std::string const &key,
    QWidget *parent = nullptr);
};

#endif
