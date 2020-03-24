#ifndef RESOURCES_H_190715
#define RESOURCES_H_190715
/* As it is not possible to create static QObjects (because the QGuiApplication
 * must preexist) we create all these constant object in a single, public
 * instance of all resources in that Resources object that's created once
 * after Qt startup. */
#include <QPixmap>

extern struct Resources *resources;

struct Resources {
  QPixmap applicationIcon;
  QPixmap lockedPixmap;
  QPixmap unlockedPixmap;
  QPixmap playPixmap;
  QPixmap waitPixmap;
  QPixmap errorPixmap;
  QPixmap infoPixmap;
  QPixmap searchPixmap;
  QPixmap closePixmap;
  QPixmap deletePixmap;
  QPixmap copyPixmap;
  QPixmap tablePixmap;
  QPixmap settingsPixmap;
  QPixmap chartPixmap;
  QPixmap upPixmap;
  QPixmap downPixmap;
  QPixmap emptyIcon;
  QPixmap factorsIcon;
  QPixmap lineChartIcon;
  QPixmap stackedChartIcon;
  QPixmap stackCenteredChartIcon;

  Resources() :
    applicationIcon(":/rmadmin.ico"),
    lockedPixmap(":/pix/locked.svg"),
    unlockedPixmap(":/pix/unlocked.svg"),
    playPixmap(":/pix/play.svg"),
    waitPixmap(":/pix/wait.svg"),
    errorPixmap(":/pix/error.svg"),
    infoPixmap(":/pix/info.svg"),
    searchPixmap(":/pix/search.svg"),
    closePixmap(":/pix/close.svg"),
    deletePixmap(":/pix/delete.svg"),
    copyPixmap(":/pix/copy.svg"),
    tablePixmap(":/pix/table.svg"),
    settingsPixmap(":/pix/settings.svg"),
    chartPixmap(":/pix/chart.svg"),
    upPixmap(":/pix/up.svg"),
    downPixmap(":/pix/down.svg"),
    emptyIcon(":/pix/empty.svg"),
    factorsIcon(":/pix/factors.svg"),
    lineChartIcon(":/pix/lineChart.svg"),
    stackedChartIcon(":/pix/stackedChart.svg"),
    stackCenteredChartIcon(":/pix/stackCenteredChart.svg")
  {}

  static Resources *get() {
    if (resources) return resources;
    resources = new Resources();
    return resources;
  }
};

#endif
