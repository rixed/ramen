#include <iostream>
#include <cmath>
#include <QChartView>
#include <QTimer>
#include <QString>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QGroupBox>
#include <QRadioButton>
#include <QCheckBox>
#include <QPieSeries>
#include "GraphModel.h"
#include "SiteItem.h"
#include "ProgramItem.h"
#include "FunctionItem.h"
#include "colorOfString.h"
#include "StoragePies.h"

using namespace QtCharts;

StoragePies::StoragePies(GraphModel *graphModel_, QWidget *parent) :
  QWidget(parent),
  graphModel(graphModel_),
  reallocTimer(this),
  dataMode(CurrentBytes),
  sumAllSites(false)
{
  QVBoxLayout *layout = new QVBoxLayout;

  QGroupBox *modeSelect = new QGroupBox(tr("Select size:"), this);
  { // the button group to select the dataMode:
    QHBoxLayout *modeLayout = new QHBoxLayout;
    QRadioButton *current = new QRadioButton(tr("&current"));
    modeLayout->addWidget(current);
    QRadioButton *alloced = new QRadioButton(tr("&allocated"));
    modeLayout->addWidget(alloced);
    QCheckBox *sum = new QCheckBox(tr("&sum all sites"));
    modeLayout->addWidget(sum);
    modeSelect->setLayout(modeLayout);

    current->setChecked(true);
    connect(current, &QRadioButton::toggled, [this](bool set) {
      dataMode = set ? CurrentBytes : AllocedBytes;
      refreshChart();
    });
    connect(sum, &QCheckBox::stateChanged, [this](bool state) {
      sumAllSites = state;
      refreshChart();
    });
  }
  layout->addWidget(modeSelect);

  QChartView *chartView = new QChartView;
  chartView->setRenderHint(QPainter::Antialiasing);
  chart = chartView->chart();
  chart->legend()->setVisible(false);

  layout->addWidget(chartView);
  // TODO: also add a selector for what to sum
  setLayout(layout);

  // Refresh the chart whenever some allocation property changes:
  reallocTimer.setSingleShot(true);
  QObject::connect(graphModel, &GraphModel::storagePropertyChanged, this, &StoragePies::rearmReallocTimer);
  QObject::connect(&reallocTimer, &QTimer::timeout, this, &StoragePies::refreshChart);
}

static int reallocTimeout = 1000;

void StoragePies::refreshChart()
{
  bool collapse[3] = { sumAllSites, false, false };

  /* First ring is keyed by site alone, of "" if collapse[0].
   * Second ring is keyed by (site or "") and (program or "").
   * Third ring is keyed etc. */
  std::map<Key, int64_t, KeyCompare> rings[3];
  QString collapsed;
  int64_t totValue = 0;

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
  radius[0] = 0.0;
  radius[1] = 0.3;
  for (unsigned r = 0; r < 2; r++) {
    radius[r+2] = std::sqrt(2*radius[r+1]*radius[r+1] - radius[r]*radius[r]);
  }
  qreal const totRadius = radius[3];
  unsigned currentRing = 0;
  int64_t minValueForLabel = totValue / 30;
  std::map<Key, StorageSlice *, KeyCompare> slices;
  chart->removeAllSeries();
  for (unsigned r = 0; r < 3; r++) {
    if (! collapse[r]) {
      QPieSeries *pie = new QPieSeries;
      pie->setHoleSize(radius[r]);
      pie->setPieSize(totRadius);
      bool const isLastRing = currentRing == numRings-1;
      for (auto it : rings[r]) {
        Key const &k = it.first;
        int64_t const v = it.second;
        QColor c(colorOfString(k.name[r]));
        c.setAlpha(isLastRing ? 200 : 25);
        bool labelVisible = v >= minValueForLabel && isLastRing;
        StorageSlice *slice =
          new StorageSlice(c, labelVisible, k, v);
        slice->setBorderWidth(1.5 * (currentRing + 1));
        slice->setBorderColor(Qt::white);
        // Set this slice on top of previous ones:
        for (auto sit : slices) {
          Key const &k_ = sit.first;
          unsigned n_;
          for (n_ = 0; n_ < 3; n_++) {
            if (k_.name[n_].length() && k_.name[n_] != k.name[n_]) break;
          }
          if (n_ == 3) sit.second->addChild(slice);
        }
        slices[k] = slice;

        connect(slice, &StorageSlice::hovered, this, &StoragePies::showDetail);
        connect(slice, SIGNAL(clicked()), this, SLOT(showDetail()));
        pie->append(slice);
      }
      chart->addSeries(pie);
      currentRing ++;
    }
  }

  // Preserve selection in between redraws:
  if (selected.isValid() && slices[selected])
    slices[selected]->setSelected(true);
}

void StoragePies::rearmReallocTimer(FunctionItem const *)
{
  reallocTimer.start(reallocTimeout);
}

void StoragePies::showDetail(bool isSelected)
{
  // For now just select the slice, but later also animate the whole thing?
  StorageSlice *slice = dynamic_cast<StorageSlice *>(sender());
  assert(slice);
  slice->setSelected(isSelected); // for now
  selected = isSelected ? slice->key : Key();
}
