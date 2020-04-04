#include <cassert>
#include <limits>
#include <QDebug>
#include "conf.h"
#include "confValue.h"
#include "misc.h"
#include "RamenValue.h"
#include "UserIdentity.h"

#include "confAutomaton.h"

static bool const verbose(false);

namespace conf {

Automaton::Automaton(
  QString const name_,
  size_t numStates,
  QObject *parent)
  : QObject(parent),
    name(name_),
    states(numStates)
{
  assert(numStates > 0);  // At least an empty state
}

void Automaton::addTransition(
  size_t fromState, size_t toState,
  unsigned keyOperations,
  std::string const &key,
  bool isPrefix,
  double timeout)
{
  assert(fromState < states.size());
  assert(toState < states.size());

  double const timeoutDate(
    timeout > 0 ?
      getTime() + timeout : std::numeric_limits<double>::max());

  states[fromState].transitions.push_back({
    toState, keyOperations, key, isPrefix, timeoutDate});
}

void Automaton::start()
{
  if (verbose)
    qInfo() << "Automaton" << name << ": starting";

  connect(kvs, &KVStore::keyChanged,
          this, &Automaton::onChange);

  tryDirectTransition();
}

void Automaton::moveTo(size_t toState)
{
  // Still check that we add this transition declared:
  for (Transition const &t : states[currentState].transitions) {
    if (t.toState == toState) {
      qInfo() << "Automaton" << name << ": forcibly moving to state" << toState;
      currentState = toState;
      emit transitionTo(this, t.toState, nullptr);
      return;
    }
  }

  qFatal("No transition for %s from %zu to %zu",
         name.toStdString().c_str(), currentState, toState);
}

void Automaton::tryTransition(
  std::string const &k, KValue const &kv, KeyOperation op)
{
  /* Ignore null values to simplify automatons */
  std::shared_ptr<RamenValueValue const> rv(
    std::dynamic_pointer_cast<RamenValueValue const>(kv.val));
  if (rv && rv->v->isNull()) {
    if (verbose)
      qDebug() << "Automaton: ignoring VNull";
    return;
  }

  if (verbose)
    qDebug() << "Automaton: Trying key" << QString::fromStdString(k)
             << "from state" << currentState;

  bool allTimedOut(true);
  double const now(getTime());

  for (Transition const &t : states[currentState].transitions) {
    if (t.timeoutDate < now) continue;
    allTimedOut = false;

    if (!(t.keyOperations & op)) continue;
    if (t.isPrefix) {
      if (!startsWith(k, t.key)) continue;
    } else {
      if (k != t.key) continue;
    }

    qInfo() << "Automaton" << name << ": transitioning to state" << t.toState;
    currentState = t.toState;
    emit transitionTo(this, t.toState, kv.val);
    break;
  }

  if (currentState >= states.size() ||
      states[currentState].transitions.size() == 0) {
    qInfo() << "Automaton" << name << ": Done";
    /* Beware: if events are queued for this object it is going to be called
     * again before it gets actually deleted! */
    deleteLater();
    return;
  }
  if (allTimedOut) {
    qWarning() << "Automaton" << name << ": Timing out";
    deleteLater();
    return;
  }

  tryDirectTransition();
}

/* If some transition wait for a lock confirmation, check that the key is not
 * owned already (in which case the Lock command is going to fail and the
 * automaton to hand there). In that case, directly transition to the next
 * state. */
void Automaton::tryDirectTransition()
{
  if (! my_uid) {
    qCritical() << "Automaton: Should not use automaton before my uid is known!";
    return;
  }

  for (Transition const &t : states[currentState].transitions) {
    if (! (t.keyOperations & OnLock)) continue;
    if (t.isPrefix) {
      /* Not necessarily an error but let's remind this limitation to
       * future self: */
      qWarning() << "Automaton: Beware thaht OnLock on a key prefix "
                    "cannot deal with already owned keys!";
      continue;
    }

    std::shared_ptr<conf::Value const> val;
    kvs->lock.lock_shared();
    auto it = kvs->map.find(t.key);
    if (it != kvs->map.end()) {
      KValue const &kv(it->second);
      if (kv.owner && *kv.owner == my_uid)
        val = kv.val;
    }
    kvs->lock.unlock_shared();

    if (val) {
      qInfo() << "Automaton" << name << ": Already own key"
              << QString::fromStdString(t.key)
              << ", transitioning to state" << t.toState;
      currentState = t.toState;
      emit transitionTo(this, t.toState, val);
      break;
    }
  }
}

void Automaton::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
        tryTransition(change.key, change.kv, OnSet);
        break;
      case KeyChanged:
        tryTransition(change.key, change.kv, OnSet);
        break;
      case KeyLocked:
        tryTransition(change.key, change.kv, OnLock);
        break;
      case KeyUnlocked:
        tryTransition(change.key, change.kv, OnUnlock);
        break;
      case KeyDeleted:
        tryTransition(change.key, change.kv, OnDelete);
        break;
    }
  }
}

};
