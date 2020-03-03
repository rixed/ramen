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
  QPixmap tablePixmap;
  QPixmap settingsPixmap;
  QPixmap chartPixmap;

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
    tablePixmap(":/pix/table.svg"),
    settingsPixmap(":/pix/settings.svg"),
    chartPixmap(":/pix/chart.svg")
  {}

  static Resources *get() {
    if (resources) return resources;
    resources = new Resources();
    return resources;
  }
};

#endif
