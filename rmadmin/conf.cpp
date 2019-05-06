#include <iostream>
#include <optional>
#include <QMap>
#include <QList>
#include <QLinkedList>
#include "conf.h"
#include "KValue.h"
#include "KWidget.h"
extern "C" {
# include <caml/mlvalues.h>
# include <caml/memory.h>
# include <caml/callback.h>
# include <caml/alloc.h>
}

namespace conf {

std::string my_uid("auth:admin");
std::string my_errors("errors/users/admin");

/* We keep all values in this map that is updated whenever a sync message is
 * received from the server: */
static QMap<std::string, KValue> kvs;

/* Another map for the callbacks.
 * Notes:
 * - a callback can be set before a value is received for that key;
 * - several widgets might edit the same key.
 */
static QMap<std::string, QList<KWidget *>> kws;

// A map of actually locked keys (as received from server), indicating who
// has the lock:
static QMap<std::string, std::string> locked;

void registerWidget(std::string const &key, KWidget *widget)
{
  std::cerr << "Registering KWidget for key " << key << std::endl;
  QList<KWidget *> &ws = kws[key];
  assert(! ws.contains(widget));
  ws.append(widget);
}

void unregisterWidget(std::string const &key, KWidget *widget)
{
  std::cerr << "Unregistering KWidget for key " << key << std::endl;
  QList<KWidget *> &ws = kws[key];
  bool was_present = ws.removeOne(widget);
  assert(was_present);
}

struct ConfRequest {
  enum Action { Lock, Unlock } action;
  std::string const key;
  std::optional<Value const> value;
};

// The ZMQ thread will pop and execute those:
QLinkedList<ConfRequest> pending_requests;

extern "C" {
  value next_pending_request()
  {
    CAMLparam0();
    CAMLlocal1(req);
    if (pending_requests.isEmpty()) {
      req = Val_int(0); // NoReq
    } else {
      ConfRequest cr = pending_requests.takeFirst();
      switch (cr.action) {
        case ConfRequest::Lock:
          req = caml_alloc(1, 0);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
        case ConfRequest::Unlock:
          req = caml_alloc(1, 1);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
      }
    }
    CAMLreturn(req);
  }
}

void askLock(std::string const &key)
{
/*
  CAMLparam0();
  CAMLlocal1(k_);
  assert(! locked.contains(key));
  static value *closure_lock = nullptr;
  if (! closure_lock) {
    closure_lock = caml_named_value("lock_from_cpp");
  }
  k_ = caml_copy_string(key.c_str());
  caml_callback(*closure_lock, k_);
  CAMLreturn0;
*/
  ConfRequest req = {
    .action = ConfRequest::Lock,
    .key = key,
    .value = std::optional<Value>()
  };
  pending_requests.push_back(req);
}

void askUnlock(std::string const &key)
{
/*
  CAMLparam0();
  CAMLlocal1(k_);
  assert(locked.contains(key)); // and belongs to my user
  static value *closure_unlock = nullptr;
  if (! closure_unlock) {
    closure_unlock = caml_named_value("unlock_from_cpp");
  }
  k_ = caml_copy_string(key.c_str());
  caml_callback(*closure_unlock, k_);
  CAMLreturn0;
*/
  ConfRequest req = {
    .action = ConfRequest::Unlock,
    .key = key,
    .value = std::optional<Value>()
  };
  pending_requests.push_back(req);
}

static void callSetValues(std::string const &key, Value const &v)
{
  QList<KWidget *> &ws = kws[key];
  for (QList<KWidget *>::const_iterator it = ws.begin();
       it != ws.end(); it++) {
    std::cerr << "setValue(" << v << ")" << std::endl;
    (*it)->setValue(v);
  }
}

static void callDelValues(std::string const &key)
{
  QList<KWidget *> &ws = kws[key];
  for (QList<KWidget *>::const_iterator it = ws.begin();
       it != ws.end(); it++) {
    (*it)->delValue(key);
  }
}

static void callLockValues(std::string const &key, std::string &uid)
{
  QList<KWidget *> &ws = kws[key];
  for (QList<KWidget *>::const_iterator it = ws.begin();
       it != ws.end(); it++) {
    (*it)->lockValue(key, uid);
  }
}

static void callUnlockValues(std::string const &key)
{
  QList<KWidget *> &ws = kws[key];
  for (QList<KWidget *>::const_iterator it = ws.begin();
       it != ws.end(); it++) {
    (*it)->unlockValue(key);
  }
}

};

#include <cassert>
extern "C" {
#  include <caml/mlvalues.h>
#  include <caml/memory.h>
#  include <caml/alloc.h>
#  include <caml/custom.h>
#  include <caml/startup.h>
#  include <caml/callback.h>

  value conf_new_key(value k_, value v_, value u_)
  {
    CAMLparam3(k_, v_, u_);
    std::string k(String_val(k_));
    conf::Value v(v_);
    std::cerr << "new key " << k << " with value " << v << std::endl;
    std::string u(String_val(u_));
    assert(! conf::kvs.contains(k));
    assert(! conf::locked.contains(k));
    conf::kvs[k] = v;
    conf::locked[k] = u;
    conf::callSetValues(k, v);
    conf::callLockValues(k, u);
    CAMLreturn(Val_unit);
  }

  value conf_set_key(value k_, value v_)
  {
    CAMLparam2(k_, v_);
    std::string k(String_val(k_));
    conf::Value v(v_);
    assert(conf::kvs.contains(k));
    conf::kvs[k] = v;
    conf::callSetValues(k, v);
    CAMLreturn(Val_unit);
  }

  value conf_del_key(value k_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    assert(conf::kvs.contains(k));
    conf::kvs.remove(k);
    // TODO: add a deleted flag on KWidgets, and disable them forever?
    conf::callDelValues(k);
    CAMLreturn(Val_unit);
  }

  value conf_lock_key(value k_, value u_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    std::string u(String_val(u_));
    assert(! conf::locked.contains(k));
    conf::locked[k] = u;
    conf::callLockValues(k, u);
    CAMLreturn(Val_unit);
  }

  value conf_unlock_key(value k_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    assert(conf::locked.contains(k));
    conf::locked.remove(k);
    conf::callUnlockValues(k);
    CAMLreturn(Val_unit);
  }
}
