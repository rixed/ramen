#include <iostream>
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

KValue& KValue::operator=(const KValue& other)
{
  owner = other.owner;
  val = other.val;
  return *this;
}

void KValue::set(conf::Key const &k, std::shared_ptr<conf::Value const> v, QString const &u, double mt)
{
  mtime = mt;
  uid = u;
  if (nullptr != val) {
    val = v;
    emit valueChanged(k, v, u, mt);
  } else {
    val = v;
    emit valueCreated(k, v, u, mt);
  }
}

void KValue::lock(conf::Key const &k, QString const &o, double ex)
{
  std::cout << "OWNER: " << k.s << " locked by " << o.toStdString() << std::endl;
  owner = o;
  expiry = ex;
  emit valueLocked(k, o, ex);
}

void KValue::unlock(conf::Key const &k)
{
  assert(owner.has_value());
  owner.reset();
  std::cout << "OWNER: " << k.s << " unlocked" << std::endl;
  emit valueUnlocked(k);
}
