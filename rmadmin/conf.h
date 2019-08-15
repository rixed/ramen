#ifndef CONF_H_190504
#define CONF_H_190504
#include <string>
#include <memory>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <map>
#include <QString>
#include <QObject>
#include "KVPair.h"
#include "rec_shared_mutex.h"

class KVStore : public QObject
{
  Q_OBJECT

public:
  std::map<std::string const, KValue> map;
  rec_shared_mutex lock;

signals:
  /* Always Once::connect to valueCreated or it will be called several times
   * on the same KValue (due to autoconnect) */
  void valueCreated(KVPair const &) const;
  void valueChanged(KVPair const &) const;
  void valueLocked(KVPair const &) const;
  void valueUnlocked(KVPair const &) const;
  void valueDeleted(KVPair const &) const;
};

extern KVStore kvs;

/* The above map is always updated by the server.
 * But we can ask the server to update a value, using those functions.
 * If we are lucky, the server will soon send an update for those keys
 * reflecting the expected change. */
void askNew(std::string const &, std::shared_ptr<conf::Value const>);
void askSet(std::string const &, std::shared_ptr<conf::Value const>);
void askLock(std::string const &);
void askUnlock(std::string const &);
void askDel(std::string const &);

std::string const changeSourceKeyExt(std::string const &key,
                                     char const *newExtension);

#endif
