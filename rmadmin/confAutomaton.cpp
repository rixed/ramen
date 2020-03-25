#include <cassert>
#include <limits>
#include <QDebug>
#include "conf.h"
#include "confValue.h"
#include "misc.h"
#include "RamenValue.h"

#include "confAutomaton.h"

static bool const verbose(true);

namespace conf {

Automaton::Automaton(
  QString const name_,
  size_t numStates,
  QObject *parent)
  : QObject(parent),
    name(name_),
    states(numStates),
    currentState(0)
{
  assert(numStates > 0);  // At least an empty state

  connect(&kvs, &KVStore::valueCreated,
          this, &Automaton::onCreate);
  connect(&kvs, &KVStore::valueChanged,
          this, &Automaton::onChange);
  connect(&kvs, &KVStore::valueLocked,
          this, &Automaton::onLock);
  connect(&kvs, &KVStore::valueUnlocked,
          this, &Automaton::onUnlock);
  connect(&kvs, &KVStore::valueDeleted,
          this, &Automaton::onDelete);
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
  }
  if (allTimedOut) {
    qWarning() << "Automaton" << name << ": Timing out";
    deleteLater();
  }
}

void Automaton::onCreate(std::string const &k, KValue const &kv)
{
  tryTransition(k, kv, OnSet);
}

void Automaton::onChange(std::string const &k, KValue const &kv)
{
  tryTransition(k, kv, OnSet);
}

void Automaton::onLock(std::string const &k, KValue const &kv)
{
  tryTransition(k, kv, OnLock);
}

void Automaton::onUnlock(std::string const &k, KValue const &kv)
{
  tryTransition(k, kv, OnUnlock);
}

void Automaton::onDelete(std::string const &k, KValue const &kv)
{
  tryTransition(k, kv, OnDelete);
}

};
