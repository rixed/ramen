#include <iostream>
#include <cmath>
#include <QChartView>
#include <QTimer>
#include <QString>
#include <QVBoxLayout>
#include <QPieSeries>
#include "GraphModel.h"
#include "SiteItem.h"
#include "ProgramItem.h"
#include "FunctionItem.h"
#include "colorOfString.h"
#include "StorageAllocs.h"

StorageAllocs::StorageAllocs(GraphModel *graphModel_, QWidget *parent) :
  QWidget(parent),
  graphModel(graphModel_),
  reallocTimer(this),
  sumAllSites(false)
{
  QVBoxLayout *layout = new QVBoxLayout(this);

  QtCharts::QChartView *chartView = new QtCharts::QChartView;
  chart = chartView->chart();
  chart->legend()->setVisible(false);

  layout->addWidget(chartView);
  // TODO: also add a selector for what to sum
  setLayout(layout);

  // Refresh the chart whenever some allocation property changes:
  reallocTimer.setSingleShot(true);
  QObject::connect(graphModel, &GraphModel::storagePropertyChanged, this, &StorageAllocs::rearmReallocTimer);
  QObject::connect(&reallocTimer, &QTimer::timeout, this, &StorageAllocs::refreshChart);
}

static int reallocTimeout = 1000;

enum DataMode { AllocedBytes, CurrentBytes };

static QString const &titleOfDataMode(DataMode dataMode)
{
  static QString const allocedBytes("Allocated Sizes");
  static QString const currentBytes("Current Sizes");
  switch (dataMode) {
    case AllocedBytes:
      return allocedBytes;
    case CurrentBytes:
      return currentBytes;
  }
}

void StorageAllocs::refreshChart()
{
  bool collapse[3] = { false, false, false };
  struct Key {
    QString const name[3];
  };
  struct KeyCompare {
    bool operator()(Key const &a, Key const &b) const
    {
      return
        a.name[0] < b.name[0] || (
          a.name[0] == b.name[0] && (
            a.name[1] < b.name[1] || (
              a.name[1] == b.name[1] &&
                a.name[2] < b.name[2])));
    }
  };

  /* First ring is keyed by site alone, of "" if collapse[0].
   * Second ring is keyed by (site or "") and (program or "").
   * Third ring is keyed etc. */
  std::map<Key, int64_t, KeyCompare> rings[3];
  QString collapsed;
  DataMode dataMode = AllocedBytes;
  int64_t totValue = 0;

  chart->setTitle(titleOfDataMode(dataMode));

  for (auto &site : graphModel->sites) {
    QString const &siteName =
      collapse[0] ? collapsed : site->name;
    Key k0 { siteName, collapsed, collapsed };
    for (auto &program : site->programs) {
      QString const &progName =
        collapse[1] ? collapsed : program->name;
      Key k1 { siteName, progName, collapsed };
      for (auto &function : program->functions) {
        QString const &funcName =
          collapse[2] ? collapsed : function->name;
        Key k2 { siteName, progName, funcName };
        int64_t v;
        switch (dataMode) {
          case AllocedBytes:
            v = function->allocArcBytes ? *function->allocArcBytes : 0;
            break;
          case CurrentBytes:
            v = function->numArcBytes ? *function->numArcBytes : 0;
            break;
        }
        rings[0][k0] += v;
        rings[1][k1] += v;
        rings[2][k2] += v;
        totValue += v;
      }
    }
  }

  unsigned numRings = 0;
  for (unsigned r = 0; r < 3; r++)
    if (! collapse[r]) numRings++;

  qreal radius[4];
  radius[0] = 0.3;
  radius[1] = 0.5;
  for (unsigned r = 0; r < 2; r++) {
    radius[r+2] = std::sqrt(2*radius[r+1]*radius[r+1] - radius[r]*radius[r]);
  }
  qreal const totRadius = radius[3];
  unsigned currentRing = 0;
  int64_t minValueForLabel = totValue / 30;
  chart->removeAllSeries();
  for (unsigned r = 0; r < 3; r++) {
    if (! collapse[r]) {
      QtCharts::QPieSeries *pie = new QtCharts::QPieSeries;
      pie->setHoleSize(radius[r]);
      pie->setPieSize(totRadius);
      bool const isLastring = currentRing == numRings-1;
      for (auto it : rings[r]) {
        Key const &k = it.first;
        int64_t const v = it.second;
        QString label = k.name[r];
        QtCharts::QPieSlice *slice = new QtCharts::QPieSlice(label, v);
        QColor c(colorOfString(label));
        c.setAlpha(isLastring ? 200 : 25);
        slice->setColor(c);
        slice->setLabelVisible(v >= minValueForLabel && isLastring);
        /*if (slice->isLabelVisible())
          std::cout << label.toStdString() << " visible @ " << v << std::endl;*/
        pie->append(slice);
      }
      chart->addSeries(pie);
      currentRing ++;
    }
  }
}

void StorageAllocs::rearmReallocTimer(FunctionItem const *)
{
  reallocTimer.start(reallocTimeout);
}
