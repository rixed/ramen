#ifndef CHART_H_190527
#define CHART_H_190527
#include <QWidget>

class QGridLayout;
class ChartDataSet;
class Graphic;

class Chart : public QWidget
{
  Q_OBJECT

  QList<ChartDataSet *> dataSets;

  QGridLayout *layout;
  Graphic *graphic;

  Graphic *defaultGraphic();

public:
  Chart(QWidget *parent = nullptr);
  ~Chart();

  // Takes ownership of the dataset:
  void addData(ChartDataSet *);

  void update();
};

#endif
