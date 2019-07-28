#ifndef CONFRCENTRY_H_190611
#define CONFRCENTRY_H_190611
#include <string>
#include <vector>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
}

namespace conf {

struct RCEntryParam;

struct RCEntry
{
  std::string programName;
  std::string source;
  std::string onSite;
  double reportPeriod;
  bool enabled;
  bool debug;
  bool automatic;
  std::vector <RCEntryParam const *> params;

  RCEntry(std::string const &programName_, bool enabled_, bool debug_,
          double reportPeriod_, std::string const &source_,
          std::string const &onSite_, bool automatic_) :
    programName(programName_),
    source(source_),
    onSite(onSite_),
    reportPeriod(reportPeriod_),
    enabled(enabled_),
    debug(debug_),
    automatic(automatic_) {}

  void addParam(RCEntryParam const *param)
  {
    params.push_back(param);
  }

  value toOCamlValue() const;
};

};

#endif
