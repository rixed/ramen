#ifndef CONFWORKERREF_H_160619
#define CONFWORKERREF_H_160619
#include <QString>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}

namespace conf {

struct WorkerRef
{
  QString const site, program, function;

  WorkerRef(QString const &site_, QString const &program_, QString const &function_) :
    site(site_), program(program_), function(function_) {}

  static WorkerRef *ofOCamlValue(value);

  QString const toQString() const;
};

};

#endif
