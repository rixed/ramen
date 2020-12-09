#ifndef ALERTINFO_H_190816
#define ALERTINFO_H_190816
#include <list>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include <QString>

class AlertInfoEditor;
class FilterEditor;
class QWidget;
struct SimpleFilter;
struct Threshold;

struct AlertInfo {
  std::string table;
  std::string column;
  bool isEnabled;
  std::list<SimpleFilter> where;
  std::optional<std::set<std::string>> groupBy;
  std::list<SimpleFilter> having;
  std::unique_ptr<Threshold const> threshold;
  double hysteresis;
  double duration;
  double ratio;
  double timeStep;  // 0 for unset
  std::list<std::string> tops;
  std::list<std::string> carryFields;
  std::list<std::pair<std::string, std::string>> carryCsts;
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

  ThresholdDistance(value);
  value toOCamlValue() const;
};

struct Threshold {
  virtual ~Threshold() {}

  static std::unique_ptr<Threshold const> ofOCaml(value);
  virtual value toOCamlValue() const = 0;
  virtual bool operator==(Threshold const &) const = 0;
};

struct ConstantThreshold : Threshold {
  double v;

  ConstantThreshold(double);
  value toOCamlValue() const override;
  bool operator==(Threshold const &) const override;
};

struct Baseline : Threshold {
  double avgWindow;
  int sampleSize;
  double percentile;
  int seasonality;
  double smoothFactor;
  ThresholdDistance const &maxDistance;

  Baseline(double, int, double, int, double, ThresholdDistance const &);
  value toOCamlValue() const override;
  bool operator==(Threshold const &) const override;
};

#endif
