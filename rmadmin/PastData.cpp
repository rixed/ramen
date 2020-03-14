#include <algorithm>
#include <QDebug>
#include "PastData.h"

static bool const verbose = true;

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

  for (ReplayRequest const &c : replayRequests) {
    // As the list is ordered by time:
    if (c.since >= until) break;
    if (c.until <= since) continue;

    if (c.until > since && c.since <= since)
      since = c.until;
    if (c.since < until && c.until >= until)
      until = c.since;
    if (since >= until) {
      if (verbose)
        qDebug() << "All time range already cached.";
      return;
    }
    // Ignore small chunks entirely within the requested interval
  }

  if (verbose)
    qDebug() << "Enqueuing a new ReplayRequest (since=" << since
             << ", until=" << until << ")";
  replayRequests.emplace_back(
    site, program, function, since, until, type, eventTime);
}

void PastData::iterTuples(
  double since, double until, bool onePast,
  std::function<void (double, std::shared_ptr<RamenValue const>)> cb) const
{
  for (ReplayRequest const &c : replayRequests) {
    if (c.since >= until) break;
    if (c.until <= since) continue;

    std::pair<double, std::shared_ptr<RamenValue const>> const *last(nullptr);
    for (size_t i = 0; i < c.tuples.size(); i++) {
      std::pair<double, std::shared_ptr<RamenValue const>> const *tuple(
        &c.tuples[i]);
      if (tuple->first < since) {
        if (onePast) last = tuple;
      } else if (tuple->first < until) {
        if (last) {
          cb(last->first, last->second);
          last = nullptr;
        }
        cb(tuple->first, tuple->second);
      } else {
        if (onePast)
          cb(tuple->first, tuple->second);
        break;
      }
    }
  }
}
