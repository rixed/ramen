#include <cassert>
#include <regex>
#include <QDebug>
#include <QLinkedList>
#include <QtGlobal>
#include <QTimer>
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
#include "RamenValue.h"

#include "conf.h"

static bool const verbose(false);

// The global KV-store:
KVStore *kvs;

KVStore::KVStore(QObject *parent) : QObject(parent)
{
  signalTimer = new QTimer(this);
  connect(signalTimer, &QTimer::timeout,
          this, QOverload<>::of(&KVStore::signalChanges));
  signalTimer->start(300);
}

void KVStore::signalChanges()
{
  confChangesLock.lock();

  if (confChanges.isEmpty()) {
    confChangesLock.unlock();
    return;
  }

  if (verbose)
    qDebug() << "KVStore: signalChanges:" << confChanges.length() << "changes";

  QList<ConfChange> confChangesCopy(std::move(confChanges));
  assert(confChanges.isEmpty());
  confChangesLock.unlock();

  emit keyChanged(confChangesCopy);
}

bool KVStore::contains(std::string const &key)
{
  lock.lock_shared();
  bool const ret(map.find(key) != map.end());
  lock.unlock_shared();
  return ret;
}

std::shared_ptr<conf::Value const> KVStore::get(std::string const &key)
{
  std::shared_ptr<conf::Value const> ret;
  lock.lock_shared();
  auto it = map.find(key);
  if (it != map.end()) ret = it->second.val;
  lock.unlock_shared();
  return ret;
}

struct ConfRequest {
  enum Action { New, Set, Lock, LockOrCreate, Unlock, Del } action;
  std::string const key;
  std::shared_ptr<conf::Value const> value;
  double timeout;  // duration of the lock for New and Lock
};

// The ZMQ thread will pop and execute those:
static QLinkedList<ConfRequest> pendingRequests;
static std::mutex pendingRequestsLock;

extern "C" {
  bool initial_sync_finished = false;
  bool exiting = false;

  // Just sets a global flag
  value conf_sync_finished()
  {
    CAMLparam0();
    /* Beware that some of the emitted signals for KV changes might not
     * have been processed yet. */
    initial_sync_finished = true;
    CAMLreturn(Val_unit);
  }

  // This _does_ alloc on the OCaml heap but is called from OCaml thread
  value next_pending_request()
  {
    CAMLparam0();
    CAMLlocal1(req);
    std::lock_guard<std::mutex> guard(pendingRequestsLock);
    if (exiting || pendingRequests.isEmpty()) {
      req = Val_int(0); // NoReq
    } else {
      if (verbose)
        qDebug() << "Popping a pending request...";
      ConfRequest cr = pendingRequests.takeFirst();
      switch (cr.action) {
        case ConfRequest::New:
          if (verbose)
            qDebug() << "...a New for" << QString::fromStdString(cr.key) << "="
                     << cr.value->toQString(cr.key);
          req = caml_alloc(3, 0);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          Store_field(req, 1, cr.value->toOCamlValue());
          Store_field(req, 2, caml_copy_double(cr.timeout));
          break;
        case ConfRequest::Set:
          if (verbose)
            qDebug() << "...a Set for" << QString::fromStdString(cr.key) << "="
                     << cr.value->toQString(cr.key);
          req = caml_alloc(2, 1);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          Store_field(req, 1, cr.value->toOCamlValue());
          break;
        case ConfRequest::Lock:
          if (verbose)
            qDebug() << "...a Lock for" << QString::fromStdString(cr.key);
          req = caml_alloc(2, 2);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          Store_field(req, 1, caml_copy_double(cr.timeout));
          break;
        case ConfRequest::LockOrCreate:
          if (verbose)
            qDebug() << "...a LockOrCreate for" << QString::fromStdString(cr.key);
          req = caml_alloc(2, 3);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          Store_field(req, 1, caml_copy_double(cr.timeout));
          break;
        case ConfRequest::Unlock:
          if (verbose)
            qDebug() << "...an Unlock for" << QString::fromStdString(cr.key);
          req = caml_alloc(1, 4);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
        case ConfRequest::Del:
          if (verbose)
            qDebug() << "...a Del for" << QString::fromStdString(cr.key);
          req = caml_alloc(1, 5);
          Store_field(req, 0, caml_copy_string(cr.key.c_str()));
          break;
      }
    }
    CAMLreturn(req);
  }
}

void askNew(
  std::string const &key, std::shared_ptr<conf::Value const> val,
  double timeout)
{
  if (verbose)
    qDebug() << "askNew:" << QString::fromStdString(key) << "="
             << (val ? val->toQString(key) : "empty");

  // Set a placeholder null value by default:
  static std::shared_ptr<conf::RamenValueValue const> nullVal =
    std::make_shared<conf::RamenValueValue const>(new VNull);
  if (! val)
    val = std::static_pointer_cast<conf::Value const>(nullVal);

  ConfRequest req = {
    .action = ConfRequest::New,
    .key = key,
    .value = val,
    .timeout = timeout
  };
  std::lock_guard<std::mutex> guard(pendingRequestsLock);
  pendingRequests.push_back(req);
}

void askSet(std::string const &key, std::shared_ptr<conf::Value const> val)
{
  ConfRequest req = {
    .action = ConfRequest::Set,
    .key = key,
    .value = val,
    .timeout = 0.
  };
  std::lock_guard<std::mutex> guard(pendingRequestsLock);
  pendingRequests.push_back(req);
}

void askLock(std::string const &key, double timeout)
{
  ConfRequest req = {
    .action = ConfRequest::Lock,
    .key = key,
    .value = nullptr,
    .timeout = timeout
  };
  std::lock_guard<std::mutex> guard(pendingRequestsLock);
  pendingRequests.push_back(req);
}

void askUnlock(std::string const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Unlock,
    .key = key,
    .value = nullptr,
    .timeout = 0.
  };
  std::lock_guard<std::mutex> guard(pendingRequestsLock);
  pendingRequests.push_back(req);
}

void askDel(std::string const &key)
{
  ConfRequest req = {
    .action = ConfRequest::Del,
    .key = key,
    .value = nullptr,
    .timeout = 0.
  };
  std::lock_guard<std::mutex> guard(pendingRequestsLock);
  pendingRequests.push_back(req);
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

    if (! exiting) {
      std::string const k(String_val(k_));
      std::shared_ptr<conf::Value> v(conf::valueOfOCaml(v_));
      QString u(String_val(u_));
      double mt(Double_val(mt_));
      bool cw = Bool_val(cw_);
      bool cd = Bool_val(cd_);

      if (verbose) qDebug() << "New key" << QString::fromStdString(k)
                            << "with value" << *v;

      kvs->lock.lock();

      auto emplaced =
        kvs->map.emplace(std::piecewise_construct,
                         std::forward_as_tuple(k),
                         std::forward_as_tuple(v, u, mt, cw, cd));

      auto map_it = emplaced.first;
      std::string const &key = map_it->first;
      KValue &kv = map_it->second;
      bool const isNew = emplaced.second;

      if (! isNew)
        /* Not supposed to happen but better safe than sorry: */
        qCritical() << "Supposedly new key" << QString::fromStdString(key) << "is not new!";

      std::lock_guard<std::mutex> guard(kvs->confChangesLock);

      kvs->confChanges.append({ KeyCreated, key, kv });

      if (caml_string_length(o_) > 0) {
        QString o(String_val(o_));
        double ex(Double_val(ex_));
        kv.setLock(o, ex);
        kvs->confChanges.append({ KeyLocked, key, kv });
      }

      kvs->lock.unlock();
    }

    CAMLreturn(Val_unit);
  }

  value no_use_for_bytecode(value *, int)
  {
    assert(false);
  }

  value conf_set_key(value k_, value v_, value u_, value mt_)
  {
    CAMLparam4(k_, v_, u_, mt_);

    if (! exiting) {
      std::string k(String_val(k_));
      std::shared_ptr<conf::Value> v(conf::valueOfOCaml(v_));
      QString u(String_val(u_));
      double mt(Double_val(mt_));

      if (verbose) qDebug() << "Set key" << QString::fromStdString(k)
                            << "to value" << *v;

      kvs->lock.lock();

      auto it = kvs->map.find(k);
      if (it == kvs->map.end()) {
        qCritical() << "!!! Setting unknown key" << QString::fromStdString(k);
      } else {
        it->second.set(v, u, mt);
        std::lock_guard<std::mutex> guard(kvs->confChangesLock);
        kvs->confChanges.append({ KeyChanged, it->first, it->second });
      }

      kvs->lock.unlock();
    }

    CAMLreturn(Val_unit);
  }

  value conf_del_key(value k_)
  {
    CAMLparam1(k_);

    if (! exiting) {
      std::string k(String_val(k_));

      if (verbose) qDebug() << "Del key" << QString::fromStdString(k);

      kvs->lock.lock();

      auto it = kvs->map.find(k);
      if (it == kvs->map.end()) {
        qCritical() << "!!! Deleting unknown key" << QString::fromStdString(k);
      } else {
        std::lock_guard<std::mutex> guard(kvs->confChangesLock);
        kvs->confChanges.append({ KeyDeleted, it->first, it->second });
        kvs->map.erase(it);
      }

      kvs->lock.unlock();
    }
    CAMLreturn(Val_unit);
  }

  value conf_lock_key(value k_, value o_, value ex_)
  {
    CAMLparam3(k_, o_, ex_);

    if (! exiting) {
      std::string k(String_val(k_));
      assert(caml_string_length(o_) > 0);
      QString o(String_val(o_));
      double ex(Double_val(ex_));

      if (verbose) qDebug() << "Lock key" << QString::fromStdString(k);

      kvs->lock.lock();

      auto it = kvs->map.find(k);
      if (it == kvs->map.end()) {
        qCritical() << "!!! Locking unknown key" << QString::fromStdString(k);
      } else {
        it->second.setLock(o, ex);
        std::lock_guard<std::mutex> guard(kvs->confChangesLock);
        kvs->confChanges.append({ KeyLocked, it->first, it->second });
      }

      kvs->lock.unlock();
    }

    CAMLreturn(Val_unit);
  }

  value conf_unlock_key(value k_)
  {
    CAMLparam1(k_);

    if (! exiting) {
      std::string k(String_val(k_));

      if (verbose) qDebug() << "Unlock key" << QString::fromStdString(k);

      kvs->lock.lock();

      auto it = kvs->map.find(k);
      if (it == kvs->map.end()) {
        qCritical() << "!!! Unlocking unknown key" << QString::fromStdString(k);
      } else {
        it->second.setUnlock();
        std::lock_guard<std::mutex> guard(kvs->confChangesLock);
        kvs->confChanges.append({ KeyUnlocked, it->first, it->second });
      }

      kvs->lock.unlock();
    }

    CAMLreturn(Val_unit);
  }
}
