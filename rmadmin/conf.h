#ifndef CONF_H_190504
#define CONF_H_190504
#include <string>
#include <QMap>
#include <QString>
#include "KValue.h"
#include "confKey.h"

namespace conf {

extern QString my_uid;
extern conf::Key my_errors;

/* We keep all KValues in this map so that it's possible to connect
 * updates to widget slots. */
extern QMap<conf::Key, KValue> kvs;

void askSet(conf::Key const &, conf::Value const &);
void askLock(conf::Key const &);
void askUnlock(conf::Key const &);

};

#endif
