#ifndef STORAGEALLOCS_H_190522
#define STORAGEALLOCS_H_190522
#include <QWidget>
#include <QTimer>

class GraphModel;
class FunctionItem;
namespace QtCharts {
  class QChart;
}

/*
 * A Pie chart displaying how the total storage size is divided amongst sites,
 * programs, functions, sites+programs or sites+programs+function.
 */

class StorageAllocs : public QWidget
{
  Q_OBJECT

  GraphModel *graphModel;
  QTimer reallocTimer;
  QtCharts::QChart *chart;

  /* Pie Chart displaying the storage size per site+program+function or
   * program+function.
   * So up to three concentric donuts: */
  bool sumAllSites;

public:
  StorageAllocs(GraphModel *, QWidget *parent = nullptr);

private slots:
  void rearmReallocTimer(FunctionItem const *);
  void refreshChart();
};

#endif
