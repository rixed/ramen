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
std::shared_mutex kvs_lock;

struct ConfRequest {
  enum Action { New, Set, Lock, Unlock, Del } action;
  std::string const key;
  std::optional<std::shared_ptr<Value const>> value;
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
        case ConfRequest::New:
          req = caml_alloc(2, 0);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          Store_field(req, 1, (*cr.value)->toOCamlValue());
          break;
        case ConfRequest::Set:
          req = caml_alloc(2, 1);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          Store_field(req, 1, (*cr.value)->toOCamlValue());
          break;
        case ConfRequest::Lock:
          req = caml_alloc(1, 2);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
        case ConfRequest::Unlock:
          req = caml_alloc(1, 3);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
        case ConfRequest::Del:
          req = caml_alloc(1, 4);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
      }
    }
    CAMLreturn(req);
  }
}

void askNew(conf::Key const &key, std::shared_ptr<conf::Value const> val)
{
  ConfRequest req = {
    .action = ConfRequest::New,
    .key = key.s,
    .value = val
  };
  pending_requests.push_back(req);
}

void askSet(conf::Key const &key, std::shared_ptr<conf::Value const> val)
{
  ConfRequest req = {
    .action = ConfRequest::Set,
    .key = key.s,
    .value = val
  };
  pending_requests.push_back(req);
}

void askLock(conf::Key const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Lock,
    .key = key.s,
    .value = std::optional<std::shared_ptr<Value const>>()
  };
  pending_requests.push_back(req);
}

void askUnlock(conf::Key const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Unlock,
    .key = key.s,
    .value = std::optional<std::shared_ptr<Value const>>()
  };
  pending_requests.push_back(req);
}

void askDel(conf::Key const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Del,
    .key = key.s,
    .value = std::optional<std::shared_ptr<Value const>>()
  };
  pending_requests.push_back(req);
}

/* List of registered autoconnects: */
struct Autoconnect
{
  std::string const prefix;
  std::function<void (conf::Key const &, KValue const *)> cb;
};

static std::list<Autoconnect> autoconnects;

// Called from Qt threads
// FIXME: return a handler (the address of the autoconnect?) with which
//        user can unregister (in destructors...).
void autoconnect(
  std::string const &prefix,
  std::function<void (conf::Key const &, KValue const *)> cb)
{
  autoconnects.push_back({ prefix, cb });

  // As a convenience, call onCreated for every pre-existing keys:
  size_t pref_len = prefix.length();
  kvs_lock.lock_shared();
  for (auto it = kvs.constKeyValueBegin(); it != kvs.constKeyValueEnd(); it++) {
    conf::Key const &key((*it).first);
    KValue const *kv = &(*it).second;
    if (key.s.length() >= pref_len &&
        0 == key.s.compare(0, pref_len, prefix))
    {
      //std::cout << "calling autoconnect callback immediately on past object..." << std::endl;
      cb(key, kv);
      emit kv->valueCreated(key, kv->value()); // beware of deadlocks! TODO: queue this kv and emit after the unlock_shared?
    }
  }
  kvs_lock.unlock_shared();
}

// Called from the OCaml thread
// Given a new KValue, connect it to all interested parties:
static void do_autoconnect(conf::Key const &key, KValue const *kv)
{
  size_t k_len = key.s.length();
  for (Autoconnect const &ac : autoconnects) {
    size_t pref_len = ac.prefix.length();
    if (pref_len <= k_len &&
        0 == key.s.compare(0, pref_len, ac.prefix))
    {
      //std::cout << "autoconnect key " << key << std::endl;
      ac.cb(key, kv);
    }
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

  /*
   * Called at reception of commands from the server:
   */

  value conf_new_key(value k_, value v_, value u_)
  {
    CAMLparam3(k_, v_, u_);
    std::string k(String_val(k_));
    std::shared_ptr<conf::Value> v(conf::valueOfOCaml(v_));
    QString u(String_val(u_));

    //std::cout << "new key " << k << " with value " << *v << std::endl;
    // key might already be bound (to uninitialized value) due to widget
    // connecting to it.
    // Connect first, and then set the value.
    conf::kvs_lock.lock();
    conf::do_autoconnect(k, &conf::kvs[k]);
    conf::kvs[k].set(k, v);
    conf::kvs[k].lock(k, u);
    conf::kvs_lock.unlock();
    CAMLreturn(Val_unit);
  }

  value conf_set_key(value k_, value v_)
  {
    CAMLparam2(k_, v_);
    std::string k(String_val(k_));
    std::shared_ptr<conf::Value> v(conf::valueOfOCaml(v_));
    //std::cout << "set key " << k << " to value " << *v << std::endl;
    conf::kvs_lock.lock();
    assert(conf::kvs.contains(k));
    conf::kvs[k].set(k, v);
    conf::kvs_lock.unlock();
    CAMLreturn(Val_unit);
  }

  value conf_del_key(value k_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    conf::kvs_lock.lock();
    assert(conf::kvs.contains(k));
    conf::kvs.remove(k);
    conf::kvs_lock.unlock();
    // TODO: Or set to uninitialized? Or what?
    CAMLreturn(Val_unit);
  }

  value conf_lock_key(value k_, value u_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    QString u(String_val(u_));
    conf::kvs_lock.lock();
    conf::kvs[k].lock(k, u);
    conf::kvs_lock.unlock();
    CAMLreturn(Val_unit);
  }

  value conf_unlock_key(value k_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    conf::kvs_lock.lock();
    conf::kvs[k].unlock(k);
    conf::kvs_lock.unlock();
    CAMLreturn(Val_unit);
  }
}
