#include <algorithm>
#include <unistd.h>
#include <cstdlib>
#include <QtGlobal>
#include <QDebug>
#include "conf.h"
#include "confValue.h"
#include "EventTime.h"
#include "ReplayRequest.h"

static bool const verbose = true;

static unsigned respKeySeq;
std::string const respKeyPrefix(
  std::to_string(getpid()) + "_" + std::to_string(std::rand()) + "_");

static std::string const nextRespKey()
{
  assert(my_socket);

  return "clients/" + *my_socket + "/response/" + respKeyPrefix +
         std::to_string(respKeySeq++);
}

ReplayRequest::ReplayRequest(
  std::string const &site, std::string const &program,
  std::string const &function,
  double since_, double until_,
  std::shared_ptr<RamenType const> type_,
  std::shared_ptr<EventTime const> eventTime_) :
  started(std::time(nullptr)),
  respKey(nextRespKey()),
  completed(false),
  type(type_),
  eventTime(eventTime_),
  since(since_),
  until(until_)
{
  // Prepare to receive the values:
  connect(&kvs, &KVStore::valueChanged,
          this, &ReplayRequest::receiveValue);
  connect(&kvs, &KVStore::valueDeleted,
          this, &ReplayRequest::endReceived);

  // Create the response key:
  askNew(respKey);

  // Then the replay request:
  std::shared_ptr<conf::ReplayRequest const> req =
    std::make_shared<conf::ReplayRequest const>(
      site, program, function, since, until, false, respKey);

  if (verbose)
    qDebug() << "ReplayRequest::ReplayRequest():"
              << QString::fromStdString(program) << "/"
              << QString::fromStdString(function)
              << "from" << (uint64_t)since
              << "to" << (uint64_t)until;

  askSet("replay_requests", req);
}

void ReplayRequest::receiveValue(std::string const &key, KValue const &kv)
{
  if (key != respKey) return;

  if (completed) {
    qCritical() << "Replay" << QString::fromStdString(respKey)
                << "received a tuple after completion";
    // Will not be ordered properly, but better than nothing
  }

  std::shared_ptr<conf::Tuple const> tuple =
    std::dynamic_pointer_cast<conf::Tuple const>(kv.val);

  if (! tuple) {
    qCritical() << "ReplayRequest::receiveValue: a"
              << conf::stringOfValueType(kv.val->valueType)
              << "?!";
    return;
  }

  RamenValue const *val = tuple->unserialize(type);
  if (! val) {
    qCritical() << "Cannot unserialize tuple:" << *kv.val;
    return;
  }

  std::optional<double> start = eventTime->ofTuple(*val);
  if (! start.has_value()) {
    qCritical() << "Dropping tuple missing event time";
    return;
  }

  tuples.insert(std::make_pair(*start, val));
}

void ReplayRequest::endReceived()
{
  completed = true;
}
