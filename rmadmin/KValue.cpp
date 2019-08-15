#include <iostream>
#include <cassert>
#include "conf.h"
#include "KValue.h"

static bool const verbose = true;

KValue::KValue(std::string const &k_) :
  inSeq(false), k(k_), mtime(0.), can_write(false), can_del(false)
{
  if (verbose)
    std::cout << "Tracking key " << k << std::endl;
}

KValue::~KValue()
{
  if (verbose)
    std::cout << "Forgetting about key " << k << std::endl;

  if (inSeq)
    seqEntry.unlink();  // should be automatic
}

void KValue::set(std::shared_ptr<conf::Value> v, QString const &u, double mt)
{
  mtime = mt;
  uid = u;

  assert(v);

  if (val) {
    val = v;
    emit valueChanged(this);
  } else {
    val = v;

    if (! inSeq) {
      inSeq = true;
      conf::keySeq.push_back(*this);
    }
    emit valueCreated(this);
  }
}

void KValue::set(std::shared_ptr<conf::Value> v, QString const &u, double mt,
                 bool cw, bool cd)
{
  can_write = cw;
  can_del = cd;
  set(v, u, mt);
}

void KValue::lock(QString const &o, double ex)
{
  if (verbose)
    std::cout << "KValue: " << k << " locked by " << o.toStdString() << std::endl;

  owner = o;
  expiry = ex;
  emit valueLocked(this);
}

void KValue::unlock()
{
  assert(owner.has_value());
  owner.reset();
  if (verbose)
    std::cout << "KValue: " << k << " unlocked" << std::endl;
  emit valueUnlocked(this);
}
