#ifndef CHART_H_190527
#define CHART_H_190527
/* A Chart is a collection of ChartDataSets (ie. vectors of values).
 * It has a Graphic (graphical representation such as a plot or a pie) that
 * can be updated for another one to explore the data sets.
 * It also has a set of controls to pick time range, colors (specific
 * graphics may have additional controls) */
#include <QWidget>

class QVBoxLayout;
class ChartDataSet;
class Graphic;
class TimeIntervalEdit;

class Chart : public QWidget
{
  friend class TimeSeries;

  Q_OBJECT

  QVBoxLayout *layout;
  Graphic *graphic;

  Graphic *defaultGraphic();

  /* Controls: */

  TimeIntervalEdit *timeIntervalEdit;

protected:
  QList<ChartDataSet *> dataSets;

public:
  Chart(QWidget *parent = nullptr);
  ~Chart();

  // Takes ownership of the dataset:
  void addData(ChartDataSet *);

  // Remove all previously added data sets:
  void reset();

public slots:
  // Update the graphic after adding/removing a dataset:
  void updateGraphic();

  // Update the chosen graphic when controls have changed or points were added:
  void updateChart();
};

#endif
