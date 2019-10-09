#ifndef TIMERANGE_H_191008
#define TIMERANGE_H_191008

struct TimeRange {
  double since, until;

  // Means empty:
  TimeRange() : since(-1.), until(-1.) {}

  TimeRange(double since_, double until_) :
    since(since_), until(until_) {}

  bool isEmpty() const { return since >= until; }

  bool contains(double t) const { return t >= since && t < until; }

  // TODO: proper list of ranges:
  void merge(TimeRange const &that)
  {
    since = std::min(since, that.since);
    until = std::max(until, that.until);
  }

  void chop(TimeRange const &have)
  {
    if (isEmpty()) return;
    if (have.since <= since && have.until > since)
      since = have.until;
    if (have.until >= until && have.since < until)
      until = have.since;
    /* TODO: make TimeRange a list of ranges and make a hole in it when
     * have is contained in this range? */
  }
};

#endif
