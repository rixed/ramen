#include <cassert>
#include <QDateTime>
#include "TimeRange.h"

double TimeRange::now() const
{
  return QDateTime::currentDateTime().toSecsSinceEpoch();
}

void TimeRange::range(double *since_, double *until_) const
{
  double origin = relative ? now() : 0;
  *since_ = origin + since;
  *until_ = origin + until;
}

bool TimeRange::contains(double t) const
{
  double origin = relative ? now() : 0;
  return t >= origin + since && t < origin + until;
}

void TimeRange::merge(TimeRange const &that)
{
  assert(relative == that.relative);  // or TODO
  since = std::min(since, that.since);
  until = std::max(until, that.until);
}

void TimeRange::chop(TimeRange const &have)
{
  assert(relative == have.relative);  // or TODO
  if (isEmpty()) return;
  if (have.since <= since && have.until > since)
    since = have.until;
  if (have.until >= until && have.since < until)
    until = have.since;
  /* TODO: make TimeRange a list of ranges and make a hole in it when
   * have is contained in this range? */
}
