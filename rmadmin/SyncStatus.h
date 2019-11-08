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

struct SyncStatus {
  SyncStatus();
  SyncStatus(value s_);
  ~SyncStatus();
  QString message();
  bool isError() const;
  bool isOk() const;

  enum Status {
    Undef, InitStart, InitOk, InitFail, Ok, Fail
  } status;
private:
  std::string msg;
};
#endif
