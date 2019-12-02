#include <algorithm>
#include <QDebug>
#include "PastData.h"

static bool verbose = true;

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

  for (PendingReplayRequest const &c : replayRequests) {
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

  replayRequests.emplace_back(
    site, program, function, since, until, type, eventTime);
}

void PastData::iterTuples(
  double since, double until,
  std::function<void (std::shared_ptr<RamenValue const>)> cb) const
{
  for (PendingReplayRequest const &c : replayRequests) {
    if (c.since >= until) break;
    if (c.until <= since) continue;

    for (std::pair<double, std::shared_ptr<RamenValue const>> t : c.tuples) {
      if (t.first >= since && t.first < until) cb(t.second);
    }
  }
}
