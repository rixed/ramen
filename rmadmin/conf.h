#ifndef CONF_H_190504
#define CONF_H_190504
#include <string>
#include <memory>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <map>
#include <boost/intrusive/list.hpp>
#include <QString>
#include <QObject>
#include "KValue.h"
#include "rec_shared_mutex.h"

namespace conf {

/* We keep all KValues in this map so that it's possible to connect
 * updates to widget slots.
 * This is accessed read/write from the OCaml thread and read only from the
 * Qt thread(s)., thus the shared_mutex: */

/*struct KeyKValue {
  boost::intrusive::list_member_hook<
    boost::intrusive::link_mode<boost::intrusive::auto_unlink>
  > kvs_entry;

  conf::Key key;
  KValue kv;
  bool inSeq; // tells if the kvs_entry has been enqueued already

  KeyKValue(conf::Key const &key_) : key(key_), inSeq(false) {}
};*/

extern std::map<std::string const, KValue> kvs;

/* The list chaining all KValues present in kvs in order of appearance,
 * repeating the key so we can iterate over that list only: */
extern boost::intrusive::list<
  KValue,
  boost::intrusive::member_hook<
    KValue,
    boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::auto_unlink>
    >,
    &KValue::seqEntry
  >,
  boost::intrusive::constant_time_size<false>
> keySeq;

extern rec_shared_mutex kvs_lock;

/* The above map is always updated by the server.
 * But we can ask the server to update a value, using those functions.
 * If we are lucky, the server will soon send an update for those keys
 * reflecting the expected change. */
void askNew(std::string const &, std::shared_ptr<conf::Value const>);
void askSet(std::string const &, std::shared_ptr<conf::Value const>);
void askLock(std::string const &);
void askUnlock(std::string const &);
void askDel(std::string const &);

/* Also, instead of connecting to individual KValues in the map to the
 * GUI actions, it's simpler to register interest for some pattern of keys
 * and be automatically connected whenever a new key arrives (otherwise there
 * would be a chicken/egg issue for new keys).
 * As an additional help, these functions will also call the given onCreated
 * for preexisting values. */
void autoconnect(
  std::string const &pattern, std::function<void (std::string const &, KValue const *)>);

Key const changeSourceKeyExt(Key const &, char const *newExtension);

};

#endif
