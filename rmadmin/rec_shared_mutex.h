#ifndef REC_SHARED_MUTEX_190612
#define REC_SHARED_MUTEX_190612
#include <cassert>
#include <shared_mutex>
#include <thread>

/* Only shared locks are recursive!
 * But the write lock also grant the read lock. */

class rec_shared_mutex : public std::shared_mutex
{
  static thread_local int count;

public:
  void lock_shared()
  {
    assert(count >= 0);
    if (count ++ == 0) {
      std::shared_mutex::lock_shared();
    }
  }

  void unlock_shared()
  {
    assert(count > 0);
    if (-- count == 0) {
      std::shared_mutex::unlock_shared();
    }
  }

  void lock()
  {
    /* No upgrade from read to write lock */
    assert(count == 0);
    std::shared_mutex::lock();
    count = 1;  // will grant a free read lock
  }

  void unlock()
  {
    count --;
    /* No downgrade from write to read: user must have returned all read
     * locks that's been taken: */
    assert(count == 0);
    std::shared_mutex::unlock();
  }
};

#endif
