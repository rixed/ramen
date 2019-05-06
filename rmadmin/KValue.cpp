#include <cassert>
#include "KValue.h"

KValue::KValue(std::string const &key_) :
  key(key_)
{
}

KValue::~KValue()
{
}

void set(conf::Value const &v)
{
  if (value.is_initialized()) {
    if (v.is_initialized()) {
      if (value != v) {
        value = v;
        emit valueChanged(key);
      }
    } else {
      value = v;
      emit valueDeleted(key);
    }
  } else {
    if (v.is_initialized()) {
      value = v;
      emit valueCreated(key);
    }
  }
}

void lock(std::string const &uid)
{
  assert(! owner);
  owner = uid;
  emit valueLocked(key, uid);
}

void unlock() const
{
  assert(owner);
  emit valueUnlocked(key);
}
