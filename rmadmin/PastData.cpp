#include <algorithm>
#include <functional>
#include <QDebug>
#include "PastData.h"

static bool const verbose(true);

/* A bit arbitrary, should depend on the tuple density: */
static double const minGapBetweenReplays(10.);

PastData::PastData(std::string const &site_, std::string const &program_,
                   std::string const &function_,
                   std::shared_ptr<RamenType const> type_,
                   std::shared_ptr<EventTime const> eventTime_,
                   QObject *parent) :
  QObject(parent),
  site(site_), program(program_), function(function_),
  type(type_), eventTime(eventTime_) {}

static void check(std::list<ReplayRequest> &replayRequests)
{
  ReplayRequest const *last = nullptr;

  for (ReplayRequest const &r : replayRequests) {
    assert(r.since < r.until);
    if (last)
      assert(last->until <= r.since);
  }
}

void PastData::request(double since, double until)
{
  if (since >= until) return;

  check(replayRequests);

  // Try to merge this new request with that previous one:
  std::function<bool(std::list<ReplayRequest>::iterator, double, double,
                     std::lock_guard<std::mutex> const &)>
    merge([this](std::list<ReplayRequest>::iterator it,
                 double since, double until,
                 std::lock_guard<std::mutex> const &guard) {
      if (! it->isWaiting(guard) ||
          since > it->until + minGapBetweenReplays ||
          until < it->since - minGapBetweenReplays) return false;

      if (verbose)
        qDebug() << qSetRealNumberPrecision(13) << "Enlarging ReplayRequest"
                 << QString::fromStdString(r.respKey) << "to"
                 << r.since << r.until;

      it->extend(since, until, guard);

      check(replayRequests);
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
      connect(&*emplaced, &ReplayRequest::tupleBatchReceived,
              this, &PastData::tupleReceived);

      check(replayRequests);
  });

  for (std::list<ReplayRequest>::iterator it = replayRequests.begin();
       it != replayRequests.end(); it++)
  {
    ReplayRequest &c(*it);
    std::lock_guard<std::mutex> guard(c.lock);

    if (c.until < since) continue;

    // As the list is ordered by time:
    if (c.since >= until) {
      if (verbose)
        qDebug() << "New request for" << qSetRealNumberPrecision(13)
                 << since << until
                 << "before" << QString::fromStdString(c.respKey);
      if (merge(it, since, until, guard)) return;
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
      if (merge(it, since, until, guard)) return;
      insert(it, since, until);
      return;
    } else if (since == c.until) {
      // New request falls right after c
      if (verbose)
        qDebug() << "New request for" << qSetRealNumberPrecision(13)
                 << since << until
                 << "right after " << QString::fromStdString(c.respKey)
                 << c.since << c.until;
      if (merge(it, since, until, guard)) return;
      // Else have a look at the following requests
    } else {
      // New request covers c entirely and must be split:
      if (verbose)
        qDebug() << "New request for" << qSetRealNumberPrecision(13)
                 << "covers " << QString::fromStdString(c.respKey);
      if (merge(it, since, until, guard)) return;
      insert(it, since, c.since);
      // And reiterate:
      since = c.until;
    }
  }

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

  check(replayRequests);

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
