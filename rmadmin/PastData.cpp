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

void PastData::request(TimeRange req)
{
  for (PendingReplayRequest const &c : pendingRequests) {
    // As the list is ordered by time:
    if (c.timeRange.since >= req.until) break;
    if (c.timeRange.until <= req.since) continue;

    if (c.timeRange.until > req.since && c.timeRange.since <= req.since)
      req.since = c.timeRange.until;
    if (c.timeRange.since < req.until && c.timeRange.until >= req.until)
      req.until = c.timeRange.since;
    if (req.since >= req.until) {
      if (verbose)
        qDebug() << "All time range already cached.";
      return;
    }
    // Ignore small chunks entirely within the requested interval
  }

  pendingRequests.emplace_back(site, program, function, req, type, eventTime);
}

void PastData::iterTuples(
  TimeRange range,
  std::function<void (std::shared_ptr<RamenValue const>)> cb) const
{
  for (PendingReplayRequest const &c : pendingRequests) {
    if (c.timeRange.since >= range.until) break;
    if (c.timeRange.until <= range.since) continue;

    for (std::pair<double, std::shared_ptr<RamenValue const>> t : c.tuples) {
      if (range.contains(t.first)) cb(t.second);
    }
  }
}
