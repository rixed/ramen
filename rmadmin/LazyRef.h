#ifndef LAZYREF_H_190509
#define LAZYREF_H_190509
#include <QDebug>
#include <functional>

/* Either a T1, or a T2 that is then cached */

template<typename T1, typename T2>
class LazyRef
{
  T1 t1;
  T2 *t2;
public:
  LazyRef() {};  // T1 must have a default constructor
  LazyRef(T1 const &t1_) : t1(t1_), t2(nullptr) {}

  ~LazyRef() {}

  LazyRef<T1, T2> &operator=(LazyRef<T1, T2> const &other)
  {
    t1 = other.t1;
    t2 = other.t2;
    return *this;
  }

  void set(T1 const &t1_)
  {
    t1 = t1_;
    t2 = nullptr;
  }

  T2 *ref(std::function<T2 *(T1 const &)> find)
  {
    if (t2) return t2;
    t2 = find(t1);
    if (t2) {
      qDebug() << "LazyRef: caching the value of" << t1;
    }
    return t2;
  }
};

#endif
