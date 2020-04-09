#include <algorithm>
#include <functional>
#include <QDebug>
#include "PastData.h"

static bool const verbose(false);

static int const maxPending(3);

/* A bit arbitrary, should depend on the tuple density: */
static double const minGapBetweenReplays(10.);

PastData::PastData(std::string const &site_, std::string const &program_,
                   std::string const &function_,
                   std::shared_ptr<RamenType const> type_,
                   std::shared_ptr<EventTime const> eventTime_,
                   double maxTime_,
                   QObject *parent) :
  QObject(parent),
  site(site_), program(program_), function(function_),
  numPending(0),
  type(type_),
  eventTime(eventTime_),
  maxTime(maxTime_)
{}

void PastData::check() const
{
  ReplayRequest const *last = nullptr;
  int numPending_(0);

  for (ReplayRequest const &r : replayRequests) {
    assert(r.since < r.until);
    if (last)
      assert(last->until <= r.since);
    last = &r;

    if (r.status != ReplayRequest::Completed) numPending_++;
  }

  assert(numPending == numPending_);
}

/* Try to merge this new request with that previous one.
 * If merge occurred (return true) then we could continue inserting
 * the new request starting from c.until. */
bool PastData::merge(
  ReplayRequest &r, ReplayRequest *next, double since, double until,
  std::lock_guard<std::mutex> const &guard)
{
  if (! r.isWaiting(guard) ||
      since > r.until + minGapBetweenReplays ||
      until < r.since - minGapBetweenReplays) return false;

  /* Extending on the left is always possible since there can be no
   * other requests in between since and r.since: */
  if (since < r.since) r.since = since;
  /* Extending on the right is more sophisticated, as there could be
   * another request on the way: */
  if (until > r.until) {
    if (next) {
      r.until = std::min(until, next->since);
    } else {
      r.until = until;
    }
  }

  if (verbose)
    qDebug() << qSetRealNumberPrecision(13) << "PastData: Enlarging ReplayRequest"
             << QString::fromStdString(r.respKey) << "to"
             << r.since << r.until;

  check();
  return true;
}

/* Either create a new ReplayRequest or queue that request for later if
 * allowed. Returns true if the request have been dealt with in any of those
 * ways. */
bool PastData::insert(
  std::list<ReplayRequest>::iterator it, double since, double until,
  bool canPostpone)
{
  if (numPending >= maxPending) {
    if (canPostpone) {
      if (verbose)
        qDebug() << "PastData: too many replay requests in flight, postponing.";
      postponedRequests.emplace_back(since, until);
      return true;
    } else {
      if (verbose)
        qDebug() << "PastData: too many replay requests in flight";
      return false;
    }

  } else {

    if (verbose)
      qDebug() << "PastData: Enqueuing a new ReplayRequest (since="
               << qSetRealNumberPrecision(13) << since
               << ", until=" << until << ")";

    std::list<ReplayRequest>::iterator const &emplaced =
      replayRequests.emplace(it,
        site, program, function, since, until, type, eventTime);
    numPending++;
    connect(&*emplaced, &ReplayRequest::tupleBatchReceived,
            this, &PastData::tupleReceived);
    connect(&*emplaced, &ReplayRequest::endReceived,
            this, &PastData::replayEnded);

    check();
    return true;
  }
}

bool PastData::request(double since, double until, bool canPostpone)
{
  if (!std::isnan(maxTime))
    until = std::min(until, maxTime);

  if (since >= until) return true;

  check();

  for (std::list<ReplayRequest>::iterator it = replayRequests.begin();
       it != replayRequests.end(); it++)
  {
    if (since >= until) return true;

    ReplayRequest &c(*it);
    ReplayRequest *next(
      std::next(it) != replayRequests.end() ?
        &*(std::next(it)) : nullptr);

    std::lock_guard<std::mutex> guard(c.lock);

    if (c.until < since) continue;

    // As the list is ordered by time:
    if (c.since >= until) {
      if (merge(c, next, since, until, guard)) return true;
      return insert(it, since, until, canPostpone);
    }

    if (c.until > since && c.since <= since)
      since = c.until;
    if (c.since < until && c.until >= until)
      until = c.since;

    if (since >= until - 1. /* Helps with epsilons */) {
      // New request falls within c
      return true;
    } else if (until == c.since) {
      // New request falls right before c
      if (merge(c, next, since, until, guard)) return true;
      return insert(it, since, until, canPostpone);
    } else if (since == c.until) {
      // New request falls right after c
      if (merge(c, next, since, until, guard)) {
        since = c.until;
      }
      // Else have a look at the following requests
    } else {
      /* New request covers c entirely and must be split.
       * Attempt to merge the beginning into c.
       * Note that even if we cannot postpone this is still beneficial
       * to merge what we can now, even if eventually we return false and
       * the same query stays postponed. */
      if (! merge(c, next, since, c.until, guard)) {
        // If impossible, add a new query for that first part:
        if (! insert(it, since, c.since, canPostpone)) return false;
      }
      // And reiterate for the remaining part:
      since = c.until;
    }
  }

  if (since >= until) return true;

  // New request falls after all previous requests
  return insert(replayRequests.end(), since, until, canPostpone);
}

void PastData::iterTuples(
  double since, double until, bool onePast,
  std::function<void (double, std::shared_ptr<RamenValue const>)> cb)
{
  double lastTime;
  std::shared_ptr<RamenValue const> last;

  check();

  for (ReplayRequest &c : replayRequests) {
    std::lock_guard<std::mutex> guard(c.lock);

    if (c.since >= until) break;
    if (c.until <= since) continue;

    for (std::pair<double, std::shared_ptr<RamenValue const>> const &tuple :
           c.tuples) {
      if (tuple.first < since) {
        if (onePast) {
          lastTime = tuple.first;
          last = tuple.second;
        }
      } else if (tuple.first < until) {
        if (last) {
          assert(lastTime <= tuple.first);
          cb(lastTime, last);
          last = nullptr;
        }
        cb(tuple.first, tuple.second);
      } else {
        if (onePast)
          cb(tuple.first, tuple.second);
        return;
      }
    }
  }
}

void PastData::replayEnded()
{
  assert(numPending > 0);
  numPending--;

  if (verbose)
    qDebug() << "PastData: a replay ended (still" << numPending << "pending)";

  if (!postponedRequests.empty()) {
    if (verbose)
      qDebug() << "PastData: retrying postponed queries";
    auto it { postponedRequests.begin() };
    while (it != postponedRequests.end()) {
      if (request(it->first, it->second, false)) {
        postponedRequests.erase(it++);
      } else {
        it++;
      }
    }
  }
}
