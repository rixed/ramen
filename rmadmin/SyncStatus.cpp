#include <cassert>
#include "SyncStatus.h"

SyncStatus::SyncStatus() : status(Undef)
{
}

SyncStatus::SyncStatus(value s_)
{
  if (Is_long(s_)) {
    switch (Long_val(s_)) {
      case 0:
        status = InitStart;
        break;
      case 1:
        status = InitOk;
        break;
      default:
        assert(!"Bad status constructor tag");
    }
  } else {
    assert(Wosize_val(s_) == 1);
    msg.assign(String_val(Field(s_, 0)));
    switch (Tag_val(s_)) {
      case 0:
        status = InitFail;
        break;
      case 1:
        status = Ok;
        break;
      case 2:
        status = Fail;
        break;
      default:
        assert(!"Tag_val(s_) <= 2");
    }
  }
}

SyncStatus::~SyncStatus()
{
}

QString SyncStatus::message()
{
  switch(status) {
    case Undef:
      return QCoreApplication::translate("QMainWindow", "undefined");
    case InitStart:
      return QCoreApplication::translate("QMainWindow", "Starting...");
    case InitOk:
      return QCoreApplication::translate("QMainWindow", "OK");
    case InitFail:
      return QCoreApplication::translate("QMainWindow", "Failed: ").
             append(msg.c_str());
    case Ok:
      return msg.c_str();
    case Fail:
      return QCoreApplication::translate("QMainWindow", "Failed: ").
             append(msg.c_str());
  }
  assert(!"Invalid sync status");
}

bool SyncStatus::isError() const
{
  return status == InitFail || status == Fail;
}

bool SyncStatus::isOk() const
{
  return status == InitOk || status == Ok;
}
