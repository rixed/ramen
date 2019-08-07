#include <iostream>
#include <regex>
#include <QLinkedList>
extern "C" {
# include <caml/mlvalues.h>
# include <caml/memory.h>
# include <caml/callback.h>
# include <caml/alloc.h>
}
#include "conf.h"

static bool const verbose = false;

namespace conf {

QMap<conf::Key, KValue> kvs;
rec_shared_mutex kvs_lock;

struct ConfRequest {
  enum Action { New, Set, Lock, LockOrCreate, Unlock, Del } action;
  std::string const key;
  std::optional<std::shared_ptr<Value const>> value;
};

// The ZMQ thread will pop and execute those:
QLinkedList<ConfRequest> pending_requests;

extern "C" {
  // This _does_ alloc on the OCaml heap but is called from OCaml thread
  value next_pending_request()
  {
    CAMLparam0();
    CAMLlocal1(req);
    if (pending_requests.isEmpty()) {
      req = Val_int(0); // NoReq
    } else {
      if (verbose)
        std::cout << "Popping a pending request..." << std::endl;
      ConfRequest cr = pending_requests.takeFirst();
      switch (cr.action) {
        case ConfRequest::New:
          if (verbose)
            std::cout << "...a New for " << cr.key << " = "
                      << (*cr.value)->toQString(conf::Key(cr.key)).toStdString() << std::endl;
          req = caml_alloc(2, 0);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          Store_field(req, 1, (*cr.value)->toOCamlValue());
          break;
        case ConfRequest::Set:
          if (verbose)
            std::cout << "...a Set for " << cr.key << " = "
                      << (*cr.value)->toQString(conf::Key(cr.key)).toStdString() << std::endl;
          req = caml_alloc(2, 1);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          Store_field(req, 1, (*cr.value)->toOCamlValue());
          break;
        case ConfRequest::Lock:
          if (verbose)
            std::cout << "...a Lock for " << cr.key << std::endl;
          req = caml_alloc(1, 2);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
        case ConfRequest::LockOrCreate:
          if (verbose)
            std::cout << "...a LockOrCreate for " << cr.key << std::endl;
          req = caml_alloc(1, 3);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
        case ConfRequest::Unlock:
          if (verbose)
            std::cout << "...an Unlock for " << cr.key << std::endl;
          req = caml_alloc(1, 4);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
        case ConfRequest::Del:
          if (verbose)
            std::cout << "...a Del for " << cr.key << std::endl;
          req = caml_alloc(1, 5);
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
  std::regex re;
  std::function<void (conf::Key const &, KValue const *)> cb;

  Autoconnect(std::string const &pattern, std::function<void (conf::Key const &, KValue const *)> cb_) :
    re(pattern, std::regex_constants::nosubs |
                std::regex_constants::optimize |
                std::regex_constants::basic), cb(cb_) {}
};

static std::list<Autoconnect> autoconnects;

// Called from Qt threads
// FIXME: return a handler (the address of the autoconnect?) with which
//        user can unregister (in destructors...).
void autoconnect(
  std::string const &pattern,
  std::function<void (conf::Key const &, KValue const *)> cb)
{
  autoconnects.emplace_back(pattern, cb);
  std::regex const &re = autoconnects.back().re;

  // As a convenience, call onCreated for every pre-existing keys:
  kvs_lock.lock_shared();
  for (auto it = kvs.constKeyValueBegin(); it != kvs.constKeyValueEnd(); it++) {
    conf::Key const &key((*it).first);
    KValue const *kv = &(*it).second;

    if (std::regex_search(key.s, re)) {
      if (verbose)
        std::cout << "Autoconnect: Key " << key.s << " matches regex, calling "
                     "callback immediately on past object..." << std::endl;
      cb(key, kv);

      /* We must signal only those connected objects that have not been
       * signaled already.
       * What we should do:
       *   1. emit valueCreated
       *   2. kv->disconnect(SIGNAL(valueCreated()));
       * What Qt should do:
       *   1. Enqueue the message into the recipient thread queue
       *   2. disconnect the signal so that no more messages are enqueued
       *   3. execute the recipient connected slot in its thread
       * What Qt seems to be doing instead:
       *   1. Enqueue the message
       *   2. disconnect the signal _and_clear_the_queue_
       *   3. Bummer!!
       *
       * So it's up to each recipients to disconnect themselves.
       * We use the https://github.com/misje/once to help with this. */
      // beware of deadlocks! TODO: queue this kv and emit after the unlock_shared?
      if (verbose)
        std::cout << "Autoconnect: Also emit the valueCreated signal" << std::endl;
      emit kv->valueCreated(key, kv->val, kv->uid, kv->mtime);
    }
  }
  kvs_lock.unlock_shared();
}

// Called from the OCaml thread
// Given a new KValue, connect it to all interested parties:
static void do_autoconnect(conf::Key const &key, KValue const *kv)
{
  for (Autoconnect const &ac : autoconnects) {
    if (std::regex_search(key.s, ac.re)) {
      if (verbose) std::cout << "autoconnect key " << key << std::endl;
      ac.cb(key, kv);
    }
  }
}

Key const changeSourceKeyExt(Key const &k, char const *newExtension)
{
  size_t lst = k.s.rfind('/');
  if (lst == std::string::npos) {
    std::cerr << "Key " << k << " is invalid for a source" << std::endl;
    assert(!"Invalid source key");
  }
  return Key(k.s.substr(0, lst + 1) + newExtension);
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

  value conf_new_key(value k_, value v_, value u_, value mt_, value cw_, value cd_,
                     value o_, value ex_)
  {
    CAMLparam5(k_, v_, u_, mt_, cw_);
    CAMLxparam3(cd_, o_, ex_);
    std::string k(String_val(k_));
    std::shared_ptr<conf::Value> v(conf::valueOfOCaml(v_));
    QString u(String_val(u_));
    double mt(Double_val(mt_));
    bool cw = Bool_val(cw_);
    bool cd = Bool_val(cd_);

    if (verbose) std::cout << "new key " << k << " with value " << *v << std::endl;
    // key might already be bound (to uninitialized value) due to widget
    // connecting to it.
    // Connect first, and then set the value.
    conf::kvs_lock.lock();

    /* First establish the signal->slot connections, then set the value: */
    conf::do_autoconnect(k, &conf::kvs[k]);
    conf::kvs[k].set(k, v, u, mt, cw, cd);

    if (caml_string_length(o_) > 0) {
      QString o(String_val(o_));
      double ex(Double_val(ex_));
      conf::kvs[k].lock(k, o, ex);
    }

    conf::kvs_lock.unlock();
    CAMLreturn(Val_unit);
  }

  value no_use_for_bytecode(value *, int)
  {
    assert(false);
  }

  value conf_set_key(value k_, value v_, value u_, value mt_)
  {
    CAMLparam4(k_, v_, u_, mt_);
    std::string k(String_val(k_));
    std::shared_ptr<conf::Value> v(conf::valueOfOCaml(v_));
    QString u(String_val(u_));
    double mt(Double_val(mt_));

    if (verbose) std::cout << "set key " << k << " to value " << *v << std::endl;
    conf::kvs_lock.lock();

    assert(conf::kvs.contains(k));
    conf::kvs[k].set(k, v, u, mt);

    conf::kvs_lock.unlock();
    CAMLreturn(Val_unit);
  }

  value conf_del_key(value k_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));
    conf::kvs_lock.lock();
    assert(conf::kvs.contains(k));
    emit conf::kvs[k].valueDeleted(k);  // better here than in the KValue destructor
    conf::kvs.remove(k);
    conf::kvs_lock.unlock();
    // TODO: Or set to uninitialized? Or what?
    CAMLreturn(Val_unit);
  }

  value conf_lock_key(value k_, value o_, value ex_)
  {
    CAMLparam3(k_, o_, ex_);
    std::string k(String_val(k_));
    assert(caml_string_length(o_) > 0);
    QString o(String_val(o_));
    double ex(Double_val(ex_));

    conf::kvs_lock.lock();
    conf::kvs[k].lock(k, o, ex);
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
