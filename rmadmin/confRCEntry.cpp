#include <iostream>
#include <cassert>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# undef alloc
}
#include "confRCEntryParam.h"
#include "confRCEntry.h"

namespace conf {

RCEntry::RCEntry(std::string const &programName_, bool enabled_, bool debug_,
                 double reportPeriod_, std::string const &source_,
                 std::string const &onSite_, bool automatic_) :
  programName(programName_),
  source(source_),
  onSite(onSite_),
  reportPeriod(reportPeriod_),
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
  std::cout << "RCEntry::toOCamlValue: debug = " << debug << std::endl;
  Store_field(ret, 1, Val_bool(debug));
  Store_field(ret, 2, caml_copy_double(reportPeriod));
  paramLst = Val_emptylist; // Aka Val_int(0)
  for (auto const param : params) {
    // prepend param:
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 1, paramLst);
    Store_field(cons, 0, param->toOCamlValue());
    paramLst = cons;
  }
  Store_field(ret, 3, paramLst);
  Store_field(ret, 4, caml_copy_string(source.c_str()));
  Store_field(ret, 5, caml_copy_string(onSite.c_str()));
  Store_field(ret, 6, Val_bool(automatic));
  CAMLreturn(ret);
}

bool RCEntry::operator==(RCEntry const &other) const
{
  if (
    programName != other.programName ||
    source != other.source ||
    onSite != other.onSite ||
    reportPeriod != other.reportPeriod ||
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
