#ifndef CONFRCENTRY_H_190611
#define CONFRCENTRY_H_190611
#include <string>
#include <vector>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}

namespace conf {

struct RCEntryParam;

struct RCEntry
{
  std::string programName;
  std::string onSite;
  double reportPeriod;
  std::string cwd;
  bool enabled;
  bool debug;
  bool automatic;
  std::vector <RCEntryParam const *> params;

  RCEntry(std::string const &programName, bool enabled, bool debug,
          double reportPeriod, std::string const &cwd,
          std::string const &onSite, bool automatic);

  void addParam(RCEntryParam const *param)
  {
    params.push_back(param);
  }

  value toOCamlValue() const;

  bool operator==(RCEntry const &other) const;
  bool operator!=(RCEntry const &other) const { return (! operator==(other)); }
};

};

#endif
