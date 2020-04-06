#ifndef SYNCSTATUS_H_20190503
#define SYNCSTATUS_H_20190503
#include <string>
#include <QtWidgets>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}

class SyncStatus {
  std::string msg;

public:
  enum Status {
    Undef, InitStart, InitOk, InitFail, Ok, Fail
  } status;

  SyncStatus();
  SyncStatus(value s_);

  ~SyncStatus();

  QString message();

  bool isError() const;

  bool isOk() const;
};

Q_DECLARE_METATYPE(SyncStatus);

#endif
