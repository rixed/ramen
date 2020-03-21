#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <mutex>
#include <unistd.h>
#include <QtGlobal>
#include <QTimer>
#include <QDebug>
#include "conf.h"
#include "confValue.h"
#include "EventTime.h"
#include "misc.h"
#include "ReplayRequest.h"

static bool const verbose(true);

static std::chrono::milliseconds const batchReplaysForMs(2000);

static unsigned respKeySeq;
static std::mutex respKeySeqLock;

std::string const respKeyPrefix(
  std::to_string(getpid()) + "_" + std::to_string(std::rand()) + "_");

static std::string const nextRespKey()
{
  assert(my_socket);

  std::lock_guard<std::mutex> guard(respKeySeqLock);

  return "clients/" + *my_socket + "/response/" + respKeyPrefix +
         std::to_string(respKeySeq++);
}

ReplayRequest::ReplayRequest(
  std::string const &site_,
  std::string const &program_,
  std::string const &function_,
  double since_, double until_,
  std::shared_ptr<RamenType const> type_,
  std::shared_ptr<EventTime const> eventTime_,
  QObject *parent)
  : QObject(parent),
    site(site_),
    program(program_),
    function(function_),
    started(std::time(nullptr)),
    respKey(nextRespKey()),
    type(type_),
    eventTime(eventTime_),
    status(Waiting),
    since(since_),
    until(until_)
{
  // Prepare to receive the values:
  connect(&kvs, &KVStore::valueChanged,
          this, &ReplayRequest::receiveValue);
  connect(&kvs, &KVStore::valueDeleted,
          this, &ReplayRequest::endReceived);

  timer = new QTimer(this);
  timer->setSingleShot(true);
  connect(timer, &QTimer::timeout,
          this, &ReplayRequest::sendRequest);
  timer->start(batchReplaysForMs);
}

bool ReplayRequest::isCompleted(std::lock_guard<std::mutex> const &) const
{
  return status == Completed;
}

bool ReplayRequest::isWaiting(std::lock_guard<std::mutex> const &) const
{
  return status == Waiting;
}

void ReplayRequest::sendRequest()
{
  std::lock_guard<std::mutex> guard(lock);

  assert(status == ReplayRequest::Waiting);
  status = Sent;

  // Create the response key:
  askNew(respKey);

  // Then the replay request:
  std::shared_ptr<conf::ReplayRequest const> req =
    std::make_shared<conf::ReplayRequest const>(
      site, program, function, since, until, false, respKey);

  if (verbose)
    qDebug() << "ReplayRequest::sendRequest:"
              << QString::fromStdString(program) << "/"
              << QString::fromStdString(function)
              << qSetRealNumberPrecision(13)
              << "from" << since << "to" << until
              << "respKey" << QString::fromStdString(respKey);

  askSet("replay_requests", req);
}

void ReplayRequest::receiveValue(std::string const &key, KValue const &kv)
{
  if (key != respKey) return;

  std::shared_ptr<conf::Tuples const> batch(
    std::dynamic_pointer_cast<conf::Tuples const>(kv.val));

  if (! batch) {
    // Probably the VNull placeholder:
    std::shared_ptr<conf::RamenValueValue const> vnull(
      std::dynamic_pointer_cast<conf::RamenValueValue const>(kv.val));
    if (! vnull || ! vnull->isNull())
      qCritical() << "ReplayRequest::receiveValue: a"
                  << conf::stringOfValueType(kv.val->valueType)
                  << "?!";
    return;
  }

  std::lock_guard<std::mutex> guard(lock);

  if (status != ReplayRequest::Sent) {
    qCritical() << "Replay" << QString::fromStdString(respKey)
                << "received a tuple while " << qstringOfStatus(status);
    // Will not be ordered properly, but better than nothing
  }

  if (verbose)
    qDebug() << "Received a batch of" << batch->tuples.size() << "tuples";

  bool hadTuple(false);

  for (conf::Tuples::Tuple const &tuple : batch->tuples) {
    RamenValue const *val = tuple.unserialize(type);
    if (! val) {
      qCritical() << "Cannot unserialize tuple:" << *kv.val;
      continue;
    }

    std::optional<double> start(eventTime->startOfTuple(*val));
    if (! start) {
      qCritical() << "Dropping tuple missing event time";
      continue;
    }

    if (!start || (*start >= since && *start <= until)) {
      tuples.insert(std::make_pair(*start, val));
      hadTuple = true;
    } else {
      std::optional<double> stop(eventTime->stopOfTuple(*val));
      if (! stop || !overlap(*start, *stop, since, until)) {
        qCritical() << "Ignoring a tuple which time" << int64_t(*start)
                    << "is not within" << int64_t(since) << "..." << int64_t(until);
      }
    }
  }

  if (hadTuple) emit tupleBatchReceived();
}

void ReplayRequest::endReceived(std::string const &key, KValue const &)
{
  if (key != respKey) return;

  if (verbose)
    qDebug() << "ReplayRequest::endReceived"
             << QString::fromStdString(respKey);

  std::lock_guard<std::mutex> guard(lock);
  status = ReplayRequest::Completed;
}

QString const ReplayRequest::qstringOfStatus(ReplayRequest::Status const status)
{
  switch (status) {
    case ReplayRequest::Waiting:
      return QString(tr("Waiting"));
    case ReplayRequest::Sent:
      return QString(tr("Sent"));
    case ReplayRequest::Completed:
      return QString(tr("Completed"));
    default:
      assert(!"Invalid status!");
  }
}
