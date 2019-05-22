#include <cassert>
#include "KValue.h"

KValue::KValue()
{
}

KValue::KValue(const KValue& other) : QObject()
{
  owner = other.owner;
  val = other.val;
}

KValue::~KValue()
{
}

std::shared_ptr<conf::Value const> KValue::value() const
{
  return val;
}

bool KValue::isSet() const
{
  return val != nullptr;
}

KValue& KValue::operator=(const KValue& other)
{
  owner = other.owner;
  val = other.val;
  return *this;
}

void KValue::set(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  if (nullptr != val) {
    if (*val != *v) {
      val = v;
      emit valueChanged(k, v);
    }
  } else {
    val = v;
    emit valueCreated(k, v);
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
