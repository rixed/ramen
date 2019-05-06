#include <iostream>
#include <optional>
#include <QLinkedList>
extern "C" {
# include <caml/mlvalues.h>
# include <caml/memory.h>
# include <caml/callback.h>
# include <caml/alloc.h>
}
#include "conf.h"

namespace conf {

QString my_uid("auth:admin");
conf::Key my_errors("errors/users/admin");

QMap<conf::Key, KValue> kvs;

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

void askLock(conf::Key const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Lock,
    .key = key.s,
    .value = std::optional<Value>()
  };
  pending_requests.push_back(req);
}

void askUnlock(conf::Key const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Unlock,
    .key = key.s,
    .value = std::optional<Value>()
  };
  pending_requests.push_back(req);
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
    QString u(String_val(u_));
    // key might already be bound (to uninitialized value) due to widget
    // connecting to it.
    conf::kvs[k].set(k, v);
    conf::kvs[k].lock(k, u);
    CAMLreturn(Val_unit);
  }

  value conf_set_key(value k_, value v_)
  {
    CAMLparam2(k_, v_);
    std::string k(String_val(k_));
    conf::Value v(v_);
    std::cerr << "set key " << k << " to value " << v << std::endl;
    assert(conf::kvs.contains(k));
    conf::kvs[k].set(k, v);
    CAMLreturn(Val_unit);
  }

  value conf_del_key(value k_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    assert(conf::kvs.contains(k));
    conf::kvs.remove(k);
    // TODO: Or set to uninitialized? Or what?
    CAMLreturn(Val_unit);
  }

  value conf_lock_key(value k_, value u_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    QString u(String_val(u_));
    conf::kvs[k].lock(k, u);
    CAMLreturn(Val_unit);
  }

  value conf_unlock_key(value k_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    conf::kvs[k].unlock(k);
    CAMLreturn(Val_unit);
  }
}
