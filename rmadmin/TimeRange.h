#ifndef TIMERANGE_H_191008
#define TIMERANGE_H_191008

class TimeRange {
  bool relative; // if true, then now must be added to since and until
  double since, until;

public:
  // Means empty:
  TimeRange() : relative(false), since(-1.), until(-1.) {}

  TimeRange(bool relative_, double since_, double until_) :
    relative(relative_), since(since_), until(until_) {}

  void range(double *since_, double *until_) const;

  bool isEmpty() const { return since >= until; }

  bool contains(double t) const;

  // TODO: proper list of ranges:
  void merge(TimeRange const &that);

  void chop(TimeRange const &have);

  static double now();
};

#endif
