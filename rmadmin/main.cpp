#include <iostream>
#include <cassert>
#include <string>
extern "C" {
#  include <caml/mlvalues.h>
#  include <caml/memory.h>
#  include <caml/alloc.h>
#  include <caml/custom.h>
#  include <caml/startup.h>
#  include <caml/callback.h>
}
#include <QApplication>
#include <QtWidgets>
#include <QMetaType>
#include <QDesktopWidget>
#include "RmAdminWin.h"
#include "SyncStatus.h"
#include "conf.h"

RmAdminWin *w = nullptr;

using namespace std;

/* Relay signals from OCaml to C++ */

extern "C" {
  value signal_conn(value url_, value status_)
  {
    CAMLparam2(url_, status_);
    string url(String_val(url_));
    SyncStatus status(status_);
    if (w) w->connProgress(status);
    CAMLreturn(Val_unit);
  }

  value signal_auth(value status_)
  {
    CAMLparam1(status_);
    SyncStatus status(status_);
    if (w) w->authProgress(status);
    CAMLreturn(Val_unit);
  }

  value signal_sync(value status_)
  {
    CAMLparam1(status_);
    SyncStatus status(status_);
    if (w) w->syncProgress(status);
    CAMLreturn(Val_unit);
  }

  static bool quit = false;
  value should_quit()
  {
    CAMLparam0();
    CAMLreturn(Val_int(quit ? 1:0));
  }
}

static void do_sync_thread(char *argv[])
{
  caml_startup(argv);
  value *start_sync = caml_named_value("start_sync");
  caml_callback(*start_sync, Val_unit);
}

int main(int argc, char *argv[])
{
  QApplication a(argc, argv);
  QCoreApplication::setApplicationName("RamenAdmin");
  qRegisterMetaType<conf::Key>();
  qRegisterMetaType<std::shared_ptr<conf::Value const>>();
  qRegisterMetaType<conf::Bool>();
  qRegisterMetaType<conf::Int>();
  qRegisterMetaType<conf::Float>();
  qRegisterMetaType<conf::String>();
  qRegisterMetaType<conf::Error>();
  qRegisterMetaType<conf::Worker>();
  qRegisterMetaType<conf::TimeRange>();
  qRegisterMetaType<conf::Retention>();

  w = new RmAdminWin();
  w->resize(QDesktopWidget().availableGeometry(w).size() * 0.75);

  thread sync_thread(do_sync_thread, argv);

  w->show();

  int ret = a.exec();
  quit = true;

  cout << "Joining with start_sync thread..." << endl;
  sync_thread.join();

  delete w;

  return ret;
}
