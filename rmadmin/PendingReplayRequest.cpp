#include <algorithm>
#include <unistd.h>
#include <cstdlib>
#include "conf.h"
#include "confValue.h"
#include "EventTime.h"
#include "PendingReplayRequest.h"

static bool const verbose = true;

static unsigned respKeySeq;
static std::string respKeyPrefix(
  std::to_string(getpid()) + "_" + std::to_string(std::rand()) + "_");

static std::string const nextRespKey()
{
  return "clients/" + *my_socket + "/response/" + respKeyPrefix +
         std::to_string(respKeySeq++);
}

PendingReplayRequest::PendingReplayRequest(
  std::string const &site, std::string const &program,
  std::string const &function,
  TimeRange timeRange_,
  std::shared_ptr<RamenType const> type_,
  std::shared_ptr<EventTime const> eventTime_) :
  started(std::time(nullptr)),
  respKey(nextRespKey()),
  completed(false),
  type(type_),
  eventTime(eventTime_),
  timeRange(timeRange_)
{
  // Prepare to receive the values:
  connect(&kvs, &KVStore::valueChanged,
          this, &PendingReplayRequest::receiveValue);
  connect(&kvs, &KVStore::valueDeleted,
          this, &PendingReplayRequest::endReceived);

  std::shared_ptr<conf::ReplayRequest const> req =
    std::make_shared<conf::ReplayRequest const>(
      site, program, function, timeRange.since, timeRange.until, respKey);

  if (verbose)
    std::cout << "PendingReplayRequest::PendingReplayRequest(): "
              << program << "/" << function << " from " << timeRange.since
              << " to " << timeRange.until << std::endl;

  askSet("replay_requests", req);
}

void PendingReplayRequest::receiveValue(std::string const &key, KValue const &kv)
{
  if (key != respKey) return;

  if (completed) {
    std::cerr << "Replay " << respKey << " received a tuple after completion"
              << std::endl;
    // Will not be ordered properly, but better than nothing
  }

  std::shared_ptr<conf::Tuple const> tuple =
    std::dynamic_pointer_cast<conf::Tuple const>(kv.val);

  if (! tuple) {
    std::cerr << "PendingReplayRequest::receiveValue: a "
              << conf::stringOfValueType(kv.val->valueType).toStdString()
              << "?!" << std::endl;
    return;
  }

  RamenValue const *val = tuple->unserialize(type);
  if (! val) {
    std::cerr << "Cannot unserialize tuple: " << *kv.val << std::endl;
    return;
  }

  std::optional<double> start = eventTime->ofTuple(*val);
  if (! start.has_value()) {
    std::cerr << "Dropping tuple missing event time" << std::endl;
    return;
  }

  tuples.emplace_back(*start, val);
}

void PendingReplayRequest::endReceived()
{
  std::sort(tuples.begin(), tuples.end(), [](auto p1, auto p2) {
    return p1.first < p2.first;
  });
  completed = true;
}
