#include <algorithm>
#include <functional>
#include <QDebug>
#include "PastData.h"

static bool const verbose(true);

static int const maxPending(3);

/* A bit arbitrary, should depend on the tuple density: */
static double const minGapBetweenReplays(10.);

PastData::PastData(std::string const &site_, std::string const &program_,
                   std::string const &function_,
                   std::shared_ptr<RamenType const> type_,
                   std::shared_ptr<EventTime const> eventTime_,
                   QObject *parent) :
  QObject(parent),
  site(site_), program(program_), function(function_),
  numPending(0),
  type(type_), eventTime(eventTime_) {}

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

void PastData::request(double since, double until)
{
  if (since >= until) return;

  if (numPending >= maxPending) {
    qWarning() << "too many in-flight replay requests";
    return;
  }

  check();

  /* Try to merge this new request with that previous one.
   * If merge occurred (return true) then we could continue inserting
   * the new request starting from c.until. */
  std::function<bool(ReplayRequest &, ReplayRequest *, double, double,
                     std::lock_guard<std::mutex> const &)>
    merge([this](ReplayRequest &r, ReplayRequest *next,
                 double since, double until,
                 std::lock_guard<std::mutex> const &guard) {
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
        qDebug() << qSetRealNumberPrecision(13) << "Enlarging ReplayRequest"
                 << QString::fromStdString(r.respKey) << "to"
                 << r.since << r.until;

      check();
      return true;
  });

  std::function<void(std::list<ReplayRequest>::iterator, double, double)>
    insert([this](std::list<ReplayRequest>::iterator it,
                  double since, double until) {
      if (verbose)
        qDebug() << "Enqueuing a new ReplayRequest (since="
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
  });

  for (std::list<ReplayRequest>::iterator it = replayRequests.begin();
       it != replayRequests.end(); it++)
  {
    if (since >= until) return;

    ReplayRequest &c(*it);
    ReplayRequest *next(
      std::next(it) != replayRequests.end() ?
        &*(std::next(it)) : nullptr);
    std::lock_guard<std::mutex> guard(c.lock);

    if (c.until < since) continue;

    // As the list is ordered by time:
    if (c.since >= until) {
      if (verbose)
        qDebug() << "New request for" << qSetRealNumberPrecision(13)
                 << since << until
                 << "before" << QString::fromStdString(c.respKey);
      if (merge(c, next, since, until, guard)) return;
      insert(it, since, until);
      return;
    }

    if (c.until > since && c.since <= since)
      since = c.until;
    if (c.since < until && c.until >= until)
      until = c.since;

    if (since >= until - 1. /* Helps with epsilons */) {
      // New request falls within c
      /*if (verbose)
        qDebug() << "Time range already in cache.";*/
      return;
    } else if (until == c.since) {
      // New request falls right before c
      if (verbose)
        qDebug() << "New request for" << qSetRealNumberPrecision(13)
                 << since << until
                 << "right before " << QString::fromStdString(c.respKey)
                 << c.since << c.until;
      if (merge(c, next, since, until, guard)) return;
      insert(it, since, until);
      return;
    } else if (since == c.until) {
      // New request falls right after c
      if (verbose)
        qDebug() << "New request for" << qSetRealNumberPrecision(13)
                 << since << until
                 << "right after " << QString::fromStdString(c.respKey)
                 << c.since << c.until;
      if (merge(c, next, since, until, guard)) {
        since = c.until;
      }
      // Else have a look at the following requests
    } else {
      // New request covers c entirely and must be split:
      if (verbose)
        qDebug() << "New request for" << qSetRealNumberPrecision(13)
                 << since << until
                 << "covers " << QString::fromStdString(c.respKey);
      // Attempt to merge the beginning into c:
      if (! merge(c, next, since, c.until, guard)) {
        // If impossible, add a new query for that first part:
        insert(it, since, c.since);
      }
      // And reiterate for the remaining part:
      since = c.until;
    }
  }

  if (since >= until) return;

  // New request falls after all previous requests
  if (verbose)
    qDebug() << "New request for" << qSetRealNumberPrecision(13)
             << since << until << "at the end";
  insert(replayRequests.end(), since, until);
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
  if (verbose)
    qDebug() << "PastData: a replay ended";

  assert(numPending > 0);
  numPending--;
}
