#include <iostream>
extern "C" {
#  include <caml/mlvalues.h>
#  include <caml/memory.h>
#  include <caml/alloc.h>
#  include <caml/custom.h>
#  include <caml/startup.h>
#  include <caml/callback.h>
}
#include <QtWidgets>
#include "RmAdminWin.h"

using namespace std;

/* Relay signals from OCaml to C++ */

extern "C" {
  value signal_conn(value url_, value status)
  {
    cerr << "signal_conn called!" << endl;
    return(Val_unit);
  }

  value signal_auth(value status)
  {
    cerr << "signal_auth called!" << endl;
    return(Val_unit);
  }

  value signal_sync(value status)
  {
    cerr << "signal_sync called!" << endl;
    return(Val_unit);
  }
}

static void do_sync_thread(char *argv[])
{
  cout << "Calling OCaml startup..." << endl;
  caml_startup(argv);
  cout << "Calling start_sync..." << endl;
  value *start_sync = caml_named_value("start_sync");
  caml_callback(*start_sync, Val_unit);
}

int main(int argc, char *argv[])
{
  cout << "Calling QApplication..." << endl;
  QApplication a(argc, argv);

  RmAdminWin w;
  w.show();

  cout << "Starting sync thread..." << endl;
  std::thread sync_thread(do_sync_thread, argv);

  cout << "Running QApplication..." << endl;
  int ret = a.exec();

  cout << "Joining with start_sync thread..." << endl;
  sync_thread.join();

  return ret;
}
