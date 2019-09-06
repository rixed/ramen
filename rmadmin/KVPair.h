#ifndef KVPAIR_H_190815
#define KVPAIR_H_190815
#include <string>
#include <utility>
#include <QObject>
#include "KValue.h"

class KVPair : public QObject
{
  Q_OBJECT

public:

  // TODO: use pointers?
  std::string const first;
  KValue second;

  KVPair(
    std::pair<std::string const, KValue> const &p,
    QObject *parent = nullptr) :
    QObject(parent), first(p.first), second(p.second) {}

  /* Metatype boilerplate: */
  KVPair(KVPair const &other) :
    QObject(),
    first(other.first),
    second(other.second) {}
  KVPair() {}
};

Q_DECLARE_METATYPE(std::string);
Q_DECLARE_METATYPE(KVPair);

#endif
