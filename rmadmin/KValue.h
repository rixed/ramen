#ifndef KVALUE_H_190506
#define KVALUE_H_190506
#include <string>
#include <optional>
#include <memory>
#include <string>
#include <boost/intrusive/list.hpp>
#include <QObject>
#include <QString>
#include "confValue.h"
#include "UserIdentity.h"

class KValue : public QObject
{
  Q_OBJECT

  /* Tells if the seqEntry above has been enqueued already.
   * TODO: use the proper intrusive list optional thing */
  bool inSeq;

public:
  std::string k;

  boost::intrusive::list_member_hook<
    boost::intrusive::link_mode<boost::intrusive::auto_unlink>
  > seqEntry;

  std::shared_ptr<conf::Value> val; // may not be set
  QString uid;  // Of the user who has set this value
  double mtime;
  std::optional<QString> owner;
  double expiry;  // if owner above is set
  bool can_write, can_del;

  /* Create a KValue with no value set (and not enqueued), useful to
   * listen to valueCreated signal: */
  KValue(std::string const &);

  ~KValue();

/*  KValue(KValue const &other) : QObject() {
    owner = other.owner;
    val = other.val;
    uid = other.uid;
    mtime = other.mtime;
    expiry = other.expiry;
    can_write = other.can_write;
    can_del = other.can_del;
  } */

  void set(std::shared_ptr<conf::Value>, QString const &, double, bool, bool);

  /* Set the value (as unlocked).
   * See lock() to signal locking and set the owner and expiry. */
  void set(std::shared_ptr<conf::Value>, QString const &, double);

  bool isSet() const {
    return val != nullptr;
  }
  bool isLocked() const {
    return isSet() && owner.has_value();
  }
  bool isMine() const {
    return isLocked() && *owner == my_uid;
  }
  void lock(QString const &, double);
  void unlock();

/*  KValue& operator=(const KValue &other) {
    owner = other.owner;
    val = other.val;
    return *this;
  } */

signals:
  /* Always Once::connect to valueCreated or it will be called several times
   * on the same KValue (due to autoconnect) */
  void valueCreated(KValue const *) const;
  void valueChanged(KValue const *) const;
  void valueLocked(KValue const *) const;
  void valueUnlocked(KValue const *) const;
  void valueDeleted(KValue const *) const;
};

#endif
