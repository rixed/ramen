#include <cassert>
#include "KValue.h"

KValue::KValue()
{
}

KValue::KValue(const KValue& other)
{
  owner = other.owner;
  val = other.val;
}

KValue::~KValue()
{
}

conf::Value const &KValue::value()
{
  return val;
}

KValue& KValue::operator=(const KValue& other)
{
  owner = other.owner;
  val = other.val;
  return *this;
}

void KValue::set(conf::Key const &k, conf::Value const &v)
{
  if (val.is_initialized()) {
    if (v.is_initialized()) {
      if (val != v) {
        val = v;
        emit valueChanged(k, v);
      }
    } else {
      val = v;
      emit valueDeleted(k);
    }
  } else {
    if (v.is_initialized()) {
      val = v;
      emit valueCreated(k, v);
    }
  }
}

void KValue::lock(conf::Key const &k, QString const &uid)
{
  assert(! owner);
  owner = uid;
  emit valueLocked(k, uid);
}

void KValue::unlock(conf::Key const &k)
{
  assert(owner);
  owner.reset();
  emit valueUnlocked(k);
}
