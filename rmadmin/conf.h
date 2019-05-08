#ifndef CONF_H_190504
#define CONF_H_190504
#include <string>
#include <memory>
#include <functional>
#include <QMap>
#include <QString>
#include <QObject>
#include "KValue.h"
#include "confKey.h"

namespace conf {

extern QString my_uid;
extern conf::Key my_errors;

/* We keep all KValues in this map so that it's possible to connect
 * updates to widget slots. */
extern QMap<conf::Key, KValue> kvs;

/* The above map is always updated by the server.
 * But we can ask the server to update a value, using those functions.
 * If we are lucky, the server will soon send an update for those keys
 * reflecting the expected change. */
void askSet(conf::Key const &, std::shared_ptr<conf::Value const>);
void askLock(conf::Key const &);
void askUnlock(conf::Key const &);

/* Also, instead of connecting to individual KValues in the map to the
 * GUI actions, it's simpler to register interest for some pattern of keys
 * and be automatically connected whenever a new key arrive (otherwise there
 * would be a chicken/egg issue for new keys).
 * As an additional help, these functions will also call the given onCreated
 * for preexisting values. */
void autoconnect(
  std::string const &prefix, std::function<void (conf::Key const &, KValue const *)>);
};

#endif