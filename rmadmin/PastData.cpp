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

void PastData::request(double since, double until)
{
  if (since >= until) return;

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
        qDebug() << "Enlarging ReplayRequest" << QString::fromStdString(it->respKey)
                 << "up to" << int64_t(until);

      it->extend(since, until, guard);

      return true;
  });

  std::function<void(std::list<ReplayRequest>::iterator, double, double)>
    insert([this](std::list<ReplayRequest>::iterator it,
                  double since, double until) {
      if (verbose)
        qDebug() << "Enqueuing a new ReplayRequest (since=" << int64_t(since)
                 << ", until=" << int64_t(until) << ")";
      replayRequests.emplace(it,
        site, program, function, since, until, type, eventTime);
  });

  for (std::list<ReplayRequest>::iterator it = replayRequests.begin();
       it != replayRequests.end(); it++)
  {
    ReplayRequest &c(*it);
    std::lock_guard<std::mutex> guard(c.lock);

    // As the list is ordered by time:
    if (c.since >= until) {
      if (verbose) qDebug() << "New request way after" << QString::fromStdString(c.respKey);
      if (merge(it, since, until, guard)) return;
      insert(it, since, until);
      return;
    }
    if (c.until <= since) continue;

    if (c.until > since && c.since <= since)
      since = c.until;
    if (c.since < until && c.until >= until)
      until = c.since;

    if (since >= until) {
      // New request falls within c
      /*if (verbose)
        qDebug() << "Time range already in cache.";*/
      return;
    } else if (until <= c.since) {
      // New request falls before c
      if (verbose) qDebug() << "New request right before " << QString::fromStdString(c.respKey);
      if (merge(it, since, until, guard)) return;
      insert(it, since, until);
      return;
    } else if (since >= c.until) {
      // New request falls after c
      if (verbose) qDebug() << "New request right after " << QString::fromStdString(c.respKey);
      if (merge(it, since, until, guard)) return;
      insert(++it, since, until);
      return;
    } else {
      // New request covers c entirely: we have to split it
      if (verbose) qDebug() << "New request cover " << QString::fromStdString(c.respKey);
      if (merge(it, since, until, guard)) return;
      insert(it, since, c.since);
      // And reiterate:
      since = c.until;
      it--;
    }
  }

  // New request falls after all previous requests
  if (verbose) qDebug() << "New request at the end";
  insert(replayRequests.end(), since, until);
}

void PastData::iterTuples(
  double since, double until, bool onePast,
  std::function<void (double, std::shared_ptr<RamenValue const>)> cb)
{
  for (ReplayRequest &c : replayRequests) {
    std::lock_guard<std::mutex> guard(c.lock);

    if (c.since >= until) break;
    if (c.until <= since) continue;

    double lastTime;
    std::shared_ptr<RamenValue const> last;
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
        break;
      }
    }
  }
}
