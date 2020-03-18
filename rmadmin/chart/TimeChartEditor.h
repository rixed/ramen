#ifndef TIMECHARTEDITOR_H_200306
#define TIMECHARTEDITOR_H_200306
#include <memory>
#include <string>
#include <QWidget>

class QPushButton;
class QWidget;
class ResizedWidget;
class TimeChart;
class TimeChartEditForm;
class TimeLineGroup;
struct TimeRange;

class TimeChartEditor : public QWidget
{
  Q_OBJECT

  TimeChartEditForm *editForm;
  TimeLineGroup *timeLineGroup;
  TimeChart *chart;
  QPushButton *openEditor;
  ResizedWidget *timeLines;

public:
  TimeChartEditor(
    std::string const &key,
    QWidget *parent = nullptr);

signals:
  void timeRangeChanged(TimeRange const &);
  void newTailTime(double);
};

/* A simple QWidget that emits a signal on resize: */
class ResizedWidget : public QWidget
{
  Q_OBJECT

  void resizeEvent(QResizeEvent *) override;

signals:
  void resized();
};

#endif
