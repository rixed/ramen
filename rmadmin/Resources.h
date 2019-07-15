#ifndef RESOURCES_H_190715
#define RESOURCES_H_190715
/* As it is not possible to create static QObjects (because the QGuiApplication
 * must preexist) we create all these constant object in a single, public
 * instance of all resources in that Resources object that's created once
 * after Qt startup. */
#include <QPixmap>

extern struct Resources *resources;

struct Resources {
  QPixmap lockedPixmap, unlockedPixmap;

  Resources() :
    lockedPixmap(":/pix/locked.svg"),
    unlockedPixmap(":/pix/unlocked.svg") {}

  static Resources *get() {
    if (resources) return resources;
    resources = new Resources();
    return resources;
  }
};

#endif
