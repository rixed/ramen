#ifndef CONF_H_190504
#define CONF_H_190504
#include <functional>
#include <memory>
#include <map>
#include <optional>
#include <string>
#include <QList>
#include <QObject>
#include <QString>
#include "KValue.h"
#include "rec_shared_mutex.h"

namespace conf {
  class Value;
}

enum ConfChangeOp { KeyCreated, KeyChanged, KeyLocked, KeyUnlocked, KeyDeleted };
struct ConfChange {
  ConfChangeOp op;
  std::string key;
  KValue kv;
};

class KVStore : public QObject
{
  Q_OBJECT

public:
  std::map<std::string const, KValue> map;
  rec_shared_mutex lock;

  bool contains(std::string const &);
  std::shared_ptr<conf::Value const> get(std::string const &);

signals:
  void keyChanged(QList<ConfChange> const &changes) const;
};

extern KVStore kvs;

extern "C" {
  extern bool initial_sync_finished;
  extern bool exiting;
};

/* The above map is always updated by the server.
 * But we can ask the server to update a value, using these functions.
 * If we are lucky, the server will soon send an update for those keys
 * reflecting the expected change. */
// If value is null then will write a placeholder VNull:
void askNew(std::string const &, std::shared_ptr<conf::Value const> = nullptr);
void askSet(std::string const &, std::shared_ptr<conf::Value const>);
void askLock(std::string const &);
void askUnlock(std::string const &);
void askDel(std::string const &);

Q_DECLARE_METATYPE(std::string); // To serialize the keys above
Q_DECLARE_METATYPE(ConfChange);

#endif
