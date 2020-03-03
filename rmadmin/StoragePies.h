#ifndef STORAGEPIES_H_190522
#define STORAGEPIES_H_190522
#include "StorageSlice.h"
#include <QTimer>
#include <QWidget>

class FunctionItem;
class GraphModel;
class QCheckBox;
class QLabel;
namespace QtCharts {
  class QChart;
}

/*
 * A Pie chart displaying how the total storage size is divided amongst sites,
 * programs, functions, sites+programs or sites+programs+function.
 */

class StoragePies : public QWidget
{
  Q_OBJECT

  GraphModel *graphModel;
  QTimer reallocTimer;
  /* Pie Chart displaying the storage size per site+program+function or
   * program+function.
   * So up to three concentric donuts: */
  QtCharts::QChart *chart;
  Key selected; // unless invalid
  bool staysSelected; // when hover ceases

  QLabel *selectionSiteLabel;
  QLabel *selectionProgLabel;
  QLabel *selectionFuncLabel;
  QLabel *selectionCurrentInfo;
  QLabel *selectionAllocInfo;
  QCheckBox *sum;

  DataMode dataMode;


  void displaySelection(Key const &, Values const &);

public:
  StoragePies(GraphModel *, QWidget *parent = nullptr);

private slots:
  void rearmReallocTimer(FunctionItem const *);
  void refreshChart();
  void toggleSelection();
  void showDetail(bool state);
  void updateSumSitesCheckBox();
};

#endif
