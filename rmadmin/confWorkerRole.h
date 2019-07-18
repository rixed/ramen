#ifndef CONFWORKERROLE_H_190618
#define CONFWORKERROLE_H_190618
#include <QString>
extern "C" {
# include <caml/mlvalues.h>
}

namespace conf {

struct WorkerRole
{
  bool isTopHalf;
  WorkerRole(bool isTopHalf_) : isTopHalf(isTopHalf_) {}

  static WorkerRole *ofOCamlValue(value);
  QString const toQString() const;
};

};

#endif
