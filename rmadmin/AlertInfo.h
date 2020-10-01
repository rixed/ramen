#ifndef ALERTINFO_H_190816
#define ALERTINFO_H_190816
#include <list>
#include <optional>
#include <memory>
#include <string>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include <QString>

class FilterEditor;
class QWidget;

struct SimpleFilter;

struct AlertInfo {
  std::string table;
  std::string column;
  bool isEnabled;
  std::list<SimpleFilter> where;
  std::optional<std::list<std::string>> groupBy;
  std::list<SimpleFilter> having;
  uniq_ptr<Threshold> threshold;
  double hysteresis;
  double duration;
  double ratio;
  double timeStep;  // 0 for unset
  std::list<std::string> tops;
  std::list<std::string> carry;
  std::string id;
  std::string descTitle;
  std::string descFiring;
  std::string descRecovery;

  ~AlertInfo() {}
  // Create an alert from an OCaml value:
  AlertInfo(value);
  // Create an alert from the editor values:
  AlertInfo(AlertInfoEditor const *);

  QString const toQString() const;
  value toOCamlValue() const;
  bool operator==(AlertInfo const &o) const;

  QWidget *editorWidget() const;
};

struct SimpleFilter {
  std::string lhs;
  std::string rhs;
  std::string op;

  SimpleFilter(value);
  SimpleFilter(FilterEditor const *);

  value toOCamlValue() const;

  bool operator==(SimpleFilter const &that) const
  {
    return lhs == that.lhs && rhs == that.rhs && op == that.op;
  }
};

struct ThresholdDistance {
  double v;
  bool relative;
};

struct Threshold {
  virtual ~Threshold() {}
};

struct ConstantThreshold : Threshold {
  double v;
};

struct Baseline : Threshold {
  double avgWindow;
  int sampleSize;
  double percentile;
  int seasonality;
  double smoothFactor;
  Distance *maxDistance;
};

#endif
