#ifndef CHART_H_190527
#define CHART_H_190527
#include <QWidget>

class QGridLayout;
class ChartDataSet;
class Graphic;

class Chart : public QWidget
{
  friend class TimeSeries;

  Q_OBJECT

  QGridLayout *layout;
  Graphic *graphic;

  Graphic *defaultGraphic();

protected:
  QList<ChartDataSet *> dataSets;

public:
  Chart(QWidget *parent = nullptr);
  ~Chart();

  // Takes ownership of the dataset:
  void addData(ChartDataSet *);

  // Remove all previously added data sets:
  void reset();

  // Update the chart for the new datasets:
  void update();
};

#endif
