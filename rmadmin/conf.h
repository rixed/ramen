#ifndef CONF_H_190504
#define CONF_H_190504
#include <functional>
#include <memory>
#include <map>
#include <mutex>
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

class QTimer;

class KVStore : public QObject
{
  Q_OBJECT

  // Signal key changes every time this timer expires:
  QTimer *signalTimer;

public:
  // The changes waiting to be signaled
  QList<ConfChange> confChanges;
  std::mutex confChangesLock;

  KVStore(QObject *parent = nullptr);

  std::map<std::string const, KValue> map;
  rec_shared_mutex lock;

  bool contains(std::string const &);
  std::shared_ptr<conf::Value const> get(std::string const &);

private slots:
  void signalChanges();

signals:
  void keyChanged(QList<ConfChange> const &changes) const;
};

extern KVStore *kvs;

extern "C" {
  extern bool initial_sync_finished;
  extern bool exiting;
};

/* Lock timeout used when a human is editing the configuration: */
#define DEFAULT_LOCK_TIMEOUT 600.

/* The above map is always updated by the server.
 * But we can ask the server to update a value, using these functions.
 * If we are lucky, the server will soon send an update for those keys
 * reflecting the expected change. */
// If value is null then will write a placeholder VNull:
void askNew(
  std::string const &, std::shared_ptr<conf::Value const> = nullptr,
  double timeout = 0.);
void askSet(std::string const &, std::shared_ptr<conf::Value const>);
void askLock(std::string const &, double timeout = DEFAULT_LOCK_TIMEOUT);
void askUnlock(std::string const &);
void askDel(std::string const &);

Q_DECLARE_METATYPE(std::string); // To serialize the keys above
Q_DECLARE_METATYPE(ConfChange);

#endif
