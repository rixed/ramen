#include <mutex>
#include <QDebug>
#include <QFile>
#include <QMetaObject>
extern "C" {
# include <caml/mlvalues.h>
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
# include <caml/startup.h>
# include <caml/callback.h>
# include <caml/threads.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "LoginWidget.h"
#include "LoginWin.h"
#include "Menu.h"
#include "RamenValue.h" // for ocamlThreadId
#include "UserIdentity.h"

static std::mutex quit_mutex;
static bool volatile quit = false;

static void set_quit()
{
  std::lock_guard<std::mutex> guard(quit_mutex);
  qDebug() << "Setting the quit flag";
  quit = true;
}

static bool get_quit()
{
  std::lock_guard<std::mutex> guard(quit_mutex);
  return quit;
}

LoginWin::LoginWin(
  QString configDir,
  QWidget *parent)
  : SavedWindow("LoginWindow", "User Authentication", false, parent)
{
  loginWidget = new LoginWidget(configDir);
  setCentralWidget(loginWidget);
  connect(loginWidget, &LoginWidget::submitted,
          this, &LoginWin::startApp);
  connect(loginWidget, &LoginWidget::cancelled,
          this, &LoginWin::exitApp);

  /* As a status bar we will have the connection status and total number
   * of exchanged messages. */
  errorMessage = new KErrorMsg(this);
  statusBar()->addPermanentWidget(errorMessage);

  /* Must not wait that the connProgress slot create the statusBar, as
   * it will be called from another thread: */
  statusBar()->showMessage(tr("Starting-up..."));

  connect(this, &LoginWin::authenticated,
          menu, &Menu::upgradeToFull);
}

LoginWin::~LoginWin()
{
  waitForOCaml();
}

void LoginWin::focusSubmit()
{
  loginWidget->focusSubmit();
}

static void call_for_new_frame(
  QString const &server, QString const &username, UserIdentity const *id)
{
  CAMLparam0();
  CAMLlocal5(srv_url_, username_,
             srv_pub_key_, clt_pub_key_, clt_priv_key_);
  srv_url_ = caml_copy_string(server.toStdString().c_str());
  username_ = caml_copy_string(username.toStdString().c_str());
# define GET(val, var) \
    val = caml_copy_string(id ? id->var.toStdString().c_str() : "");
  GET(srv_pub_key_, srv_pub_key);
  GET(clt_pub_key_, clt_pub_key);
  GET(clt_priv_key_, clt_priv_key);
  value args[5] { srv_url_, username_, srv_pub_key_, clt_pub_key_, clt_priv_key_ };
  value *start_sync = caml_named_value("start_sync");
  caml_callbackN(*start_sync, 5, args);
  CAMLreturn0;
}

// The only thread that will ever call OCaml runtime:
static void do_sync_thread(
  QString const &server, QString const &username, UserIdentity const *id)
{
  ocamlThreadId = std::this_thread::get_id();
  caml_c_thread_register();
  caml_acquire_runtime_system();
  call_for_new_frame(server, username, id);
  caml_release_runtime_system();
  caml_c_thread_unregister();
}

// Called whenever the user is authenticated in the server
void LoginWin::startApp(
  QString const server, QString const username, QString const idFile)
{
  loginWidget->setSubmitStatus("Connecting...");

  UserIdentity *id = nullptr;
  if (! idFile.isEmpty()) {
    QFile file(idFile);
    id = new UserIdentity(file);
    if (! id->isValid) {
      qCritical() << "Identity file" << idFile << "is not valid";
      delete id;
      QCoreApplication::exit(1);
      return;
    }
  }

  // Set the global my_uid
  my_uid = username;
  if (id && id->isValid) my_uid = id->username;

  /* Create the windows that will connect to the kvs->map keys *before*
   * the sync thread is started: */
  Menu::initDialogs(server);

  // Start a new thread
  qInfo() << "Starting The OCaml thread.";
  sync_thread = std::thread(do_sync_thread, server, *my_uid, id);
}

void LoginWin::exitApp()
{
  waitForOCaml();
  QCoreApplication::quit();
}

void LoginWin::waitForOCaml()
{

  set_quit();
  if (sync_thread.joinable()) {
    qDebug() << "Joining with start_sync thread...";
    sync_thread.join();
  }
}

void LoginWin::setStatusMsg()
{
  if (syncStatus.isOk()) {
    assert(my_uid.has_value());  // Because startApp is called first
    loginWidget->setSubmitStatus("Connected as " + *my_uid);
  }

  QStatusBar *sb = statusBar(); // create it if it doesn't exist yet
  if (connStatus.isError() ||
      authStatus.isError() ||
      syncStatus.isError())
  {
    sb->setStyleSheet("background-color: pink;");
  }
  QString msg;
  if (! connStatus.isOk()) {
    msg = tr("Connection: ").append(connStatus.message());
  } else if (! authStatus.isOk()) {
    msg = tr("Authentication: ").append(authStatus.message());
  } else {
    msg = tr("Synchronization: ").append(syncStatus.message());
  }

  sb->showMessage(msg);
}

void LoginWin::connProgress(SyncStatus status)
{
  connStatus = status;
  setStatusMsg();
}

void LoginWin::authProgress(SyncStatus status)
{
  authStatus = status;
  setStatusMsg();
}

void LoginWin::syncProgress(SyncStatus status)
{
  syncStatus = status;
  setStatusMsg();
  if (status.status == SyncStatus::Status::InitOk) {
    emit authenticated();
    hide();
  }
}

/* The above is called from OCaml: */

extern "C" {
# include "../src/config.h"

  /* Relay signals from OCaml to C++ */

  value signal_conn(value url_, value status_)
  {
    CAMLparam2(url_, status_);
    std::string url(String_val(url_));
    SyncStatus status(status_);
    if (Menu::loginWin) Menu::loginWin->connProgress(status);
    CAMLreturn(Val_unit);
  }

  value signal_auth(value status_)
  {
    CAMLparam1(status_);
    SyncStatus status(status_);
    if (Menu::loginWin) Menu::loginWin->authProgress(status);
    CAMLreturn(Val_unit);
  }

  value signal_sync(value status_)
  {
    CAMLparam1(status_);
    SyncStatus status(status_);
    if (Menu::loginWin) {
      if (!QMetaObject::invokeMethod(
            Menu::loginWin, "syncProgress", Qt::QueuedConnection,
            Q_ARG(SyncStatus, status))) {
        qCritical("Cannot signal synchronisation progress");
      }
    }
    CAMLreturn(Val_unit);
  }

  value should_quit()
  {
    CAMLparam0();
    bool q = get_quit();
    CAMLreturn(Val_int(q ? 1:0));
  }

  value set_my_id(value key_, value socket_)
  {
    CAMLparam2(key_, socket_);
    std::string k(String_val(key_));
    my_errors = k;
    if (Menu::loginWin) Menu::loginWin->setErrorKey(k);
    my_socket = String_val(socket_);
    CAMLreturn(Val_unit);
  }
}
