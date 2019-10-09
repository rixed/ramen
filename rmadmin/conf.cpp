#include <iostream>
#include <cassert>
#include <regex>
#include <QLinkedList>
extern "C" {
# include <caml/mlvalues.h>
# include <caml/memory.h>
# include <caml/callback.h>
# include <caml/alloc.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "confValue.h"
#include "conf.h"

static bool const verbose = true;

// The global KV-store:

KVStore kvs;

struct ConfRequest {
  enum Action { New, Set, Lock, LockOrCreate, Unlock, Del } action;
  std::string const key;
  std::optional<std::shared_ptr<conf::Value const>> value;
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
                      << (*cr.value)->toQString(cr.key).toStdString() << std::endl;
          req = caml_alloc(2, 0);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          Store_field(req, 1, (*cr.value)->toOCamlValue());
          break;
        case ConfRequest::Set:
          if (verbose)
            std::cout << "...a Set for " << cr.key << " = "
                      << (*cr.value)->toQString(cr.key).toStdString() << std::endl;
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

void askNew(std::string const &key, std::shared_ptr<conf::Value const> val)
{
  ConfRequest req = {
    .action = ConfRequest::New,
    .key = key,
    .value = val
  };
  pending_requests.push_back(req);
}

void askSet(std::string const &key, std::shared_ptr<conf::Value const> val)
{
  ConfRequest req = {
    .action = ConfRequest::Set,
    .key = key,
    .value = val
  };
  pending_requests.push_back(req);
}

void askLock(std::string const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Lock,
    .key = key,
    .value = std::optional<std::shared_ptr<conf::Value const>>()
  };
  pending_requests.push_back(req);
}

void askUnlock(std::string const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Unlock,
    .key = key,
    .value = std::optional<std::shared_ptr<conf::Value const>>()
  };
  pending_requests.push_back(req);
}

void askDel(std::string const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Del,
    .key = key,
    .value = std::optional<std::shared_ptr<conf::Value const>>()
  };
  pending_requests.push_back(req);
}

#include <cassert>
extern "C" {
#  include <caml/custom.h>
#  include <caml/startup.h>
#  undef alloc

  /*
   * Called at reception of commands from the server:
   */

  value conf_new_key(value k_, value v_, value u_, value mt_, value cw_, value cd_,
                     value o_, value ex_)
  {
    CAMLparam5(k_, v_, u_, mt_, cw_);
    CAMLxparam3(cd_, o_, ex_);
    std::string const k(String_val(k_));
    std::shared_ptr<conf::Value> v(conf::valueOfOCaml(v_));
    QString u(String_val(u_));
    double mt(Double_val(mt_));
    bool cw = Bool_val(cw_);
    bool cd = Bool_val(cd_);

    if (verbose) std::cout << "New key " << k << " with value " << *v << std::endl;

    kvs.lock.lock();

    auto emplaced =
      kvs.map.emplace(std::piecewise_construct,
                      std::forward_as_tuple(k),
                      std::forward_as_tuple(v, u, mt, cw, cd));

    auto map_it = emplaced.first;
    std::string const &key = map_it->first;
    KValue &kv = map_it->second;
    bool const isNew = emplaced.second;

    if (! isNew)
      /* Not supposed to happen but better safe than sorry: */
      std::cerr << "Supposedly new key " << key << " is not new!" << std::endl;

    emit kvs.valueCreated(key, kv);

    if (caml_string_length(o_) > 0) {
      QString o(String_val(o_));
      double ex(Double_val(ex_));
      kv.setLock(o, ex);
      emit kvs.valueLocked(key, kv);
    }

    kvs.lock.unlock();
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

    if (verbose) std::cout << "Set key " << k << " to value " << *v << std::endl;

    kvs.lock.lock();

    auto it = kvs.map.find(k);
    if (it == kvs.map.end()) {
      std::cerr << "!!! Setting unknown key " << k << std::endl;
    } else {
      it->second.set(v, u, mt);
      emit kvs.valueChanged(it->first, it->second);
    }

    kvs.lock.unlock();
    CAMLreturn(Val_unit);
  }

  value conf_del_key(value k_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));

    if (verbose) std::cout << "Del key " << k << std::endl;

    kvs.lock.lock();

    auto it = kvs.map.find(k);
    if (it == kvs.map.end()) {
      std::cerr << "!!! Deleting unknown key " << k << std::endl;
    } else {
      emit kvs.valueDeleted(it->first, it->second);
      kvs.map.erase(it);
    }

    kvs.lock.unlock();
    CAMLreturn(Val_unit);
  }

  value conf_lock_key(value k_, value o_, value ex_)
  {
    CAMLparam3(k_, o_, ex_);
    std::string k(String_val(k_));
    assert(caml_string_length(o_) > 0);
    QString o(String_val(o_));
    double ex(Double_val(ex_));

    if (verbose) std::cout << "Lock key " << k << std::endl;

    kvs.lock.lock();

    auto it = kvs.map.find(k);
    if (it == kvs.map.end()) {
      std::cerr << "!!! Locking unknown key " << k << std::endl;
    } else {
      it->second.setLock(o, ex);
      emit kvs.valueLocked(it->first, it->second);
    }

    kvs.lock.unlock();
    CAMLreturn(Val_unit);
  }

  value conf_unlock_key(value k_)
  {
    CAMLparam1(k_);
    std::string k(String_val(k_));

    if (verbose) std::cout << "Unlock key " << k << std::endl;

    kvs.lock.lock();

    auto it = kvs.map.find(k);
    if (it == kvs.map.end()) {
      std::cerr << "!!! Unlocking unknown key " << k << std::endl;
    } else {
      it->second.setUnlock();
      emit kvs.valueUnlocked(it->first, it->second);
    }

    kvs.lock.unlock();
    CAMLreturn(Val_unit);
  }
}
