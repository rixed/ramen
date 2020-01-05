#include <cassert>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# undef alloc
# undef flush
}
#include "confRCEntryParam.h"
#include "confRCEntry.h"

namespace conf {

RCEntry::RCEntry(std::string const &programName_, bool enabled_, bool debug_,
                 double reportPeriod_, std::string const &cwd_,
                 std::string const &onSite_, bool automatic_) :
  programName(programName_),
  onSite(onSite_),
  reportPeriod(reportPeriod_),
  cwd(cwd_),
  enabled(enabled_),
  debug(debug_),
  automatic(automatic_) {}

// This _does_ alloc on the OCaml heap
value RCEntry::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal3(ret, paramLst, cons);
  checkInOCamlThread();
  ret = caml_alloc_tuple(7);
  Store_field(ret, 0, Val_bool(enabled));
  Store_field(ret, 1, Val_bool(debug));
  Store_field(ret, 2, caml_copy_double(reportPeriod));
  Store_field(ret, 3, caml_copy_string(cwd.c_str()));
  paramLst = Val_emptylist; // Aka Val_int(0)
  for (auto const param : params) {
    // prepend param:
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 1, paramLst);
    Store_field(cons, 0, param->toOCamlValue());
    paramLst = cons;
  }
  Store_field(ret, 4, paramLst);
  Store_field(ret, 5, caml_copy_string(onSite.c_str()));
  Store_field(ret, 6, Val_bool(automatic));
  CAMLreturn(ret);
}

bool RCEntry::operator==(RCEntry const &other) const
{
  if (
    programName != other.programName ||
    onSite != other.onSite ||
    reportPeriod != other.reportPeriod ||
    cwd != other.cwd ||
    enabled != other.enabled ||
    debug != other.debug ||
    automatic != other.automatic
  ) {
    return false;
  }

  for (RCEntryParam const *p : params) {
    bool found = false;
    for (RCEntryParam const *op : other.params) {
      if (*p == *op) {
        found = true;
        break;
      }
    }
    if (! found) return false;
  }

  return true;
}

};
