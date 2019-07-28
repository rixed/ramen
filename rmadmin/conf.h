#ifndef CONF_H_190504
#define CONF_H_190504
#include <string>
#include <memory>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <QMap>
#include <QString>
#include <QObject>
#include "KValue.h"
#include "confKey.h"
#include "rec_shared_mutex.h"

namespace conf {

/* We keep all KValues in this map so that it's possible to connect
 * updates to widget slots.
 * This is accessed read/write from the OCaml thread and read only from the
 * Qt thread(s)., thus the shared_mutex: */
// TODO: a prefix tree
extern QMap<conf::Key, KValue> kvs;
extern rec_shared_mutex kvs_lock;

/* The above map is always updated by the server.
 * But we can ask the server to update a value, using those functions.
 * If we are lucky, the server will soon send an update for those keys
 * reflecting the expected change. */
void askNew(conf::Key const &, std::shared_ptr<conf::Value const>);
void askSet(conf::Key const &, std::shared_ptr<conf::Value const>);
void askLock(conf::Key const &);
void askUnlock(conf::Key const &);
void askDel(conf::Key const &);

/* Also, instead of connecting to individual KValues in the map to the
 * GUI actions, it's simpler to register interest for some pattern of keys
 * and be automatically connected whenever a new key arrives (otherwise there
 * would be a chicken/egg issue for new keys).
 * As an additional help, these functions will also call the given onCreated
 * for preexisting values. */
void autoconnect(
  std::string const &pattern, std::function<void (conf::Key const &, KValue const *)>);
};

#endif
