#ifndef ALERTINFO_H_190816
#define ALERTINFO_H_190816
#include <list>
#include <string>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include <QString>

class FilterEditor;
class QItemSelection;
class QLineEdit;
class QWidget;

struct AlertInfo {
  virtual ~AlertInfo() {}
  virtual QString const toQString() const = 0;
  virtual value toOCamlValue() const = 0;
  virtual bool operator==(AlertInfo const &o) const = 0;
};

class AlertInfoV1Editor;
struct SimpleFilter;

struct AlertInfoV1 : public AlertInfo
{
  std::string table;
  std::string column;
  bool isEnabled;
  std::list<SimpleFilter> where;
  std::list<SimpleFilter> having;
  double threshold;
  double recovery;
  double duration;
  double ratio;
  double timeStep;
  std::list<std::string> tops;
  std::string id;
  std::string descTitle;
  std::string descFiring;
  std::string descRecovery;

  // Create an alert from an OCaml value:
  AlertInfoV1(value);

  // Create an alert from the editor values:
  AlertInfoV1(AlertInfoV1Editor const *);

  value toOCamlValue() const;

  QWidget *editorWidget() const;

  QString const toQString() const;

  bool operator==(AlertInfoV1 const &) const;
  bool operator==(AlertInfo const &) const;
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

#endif
