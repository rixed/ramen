#ifndef STORAGEPIES_H_190522
#define STORAGEPIES_H_190522
#include "StorageSlice.h"
#include <QWidget>
#include <QTimer>

class GraphModel;
class FunctionItem;
namespace QtCharts {
  class QChart;
}

enum DataMode { AllocedBytes, CurrentBytes };

/*
 * A Pie chart displaying how the total storage size is divided amongst sites,
 * programs, functions, sites+programs or sites+programs+function.
 */

class StoragePies : public QWidget
{
  Q_OBJECT

  GraphModel *graphModel;
  QTimer reallocTimer;
  QtCharts::QChart *chart;
  Key selected; // unless invalid

  DataMode dataMode;

  /* Pie Chart displaying the storage size per site+program+function or
   * program+function.
   * So up to three concentric donuts: */
  bool sumAllSites;

public:
  StoragePies(GraphModel *, QWidget *parent = nullptr);

private slots:
  void rearmReallocTimer(FunctionItem const *);
  void refreshChart();
public slots:
  void showDetail(bool state = true);
};

#endif
