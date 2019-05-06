#ifndef CONFVALUE_H_190504
#define CONFVALUE_H_190504
#include <iostream>
#include <QString>
#include <QMetaType>
#include <QDebug>
extern "C" {
#  include <caml/mlvalues.h>
}

namespace conf {

struct Error {
  double time;
  unsigned cmd_id;
  std::string msg;
  friend bool operator==(Error const &, Error const &);
  friend bool operator!=(Error const &, Error const &);
};

struct Retention {
  double duration;
  double period;
  friend bool operator==(Retention const &, Retention const &);
  friend bool operator!=(Retention const &, Retention const &);
};

class Value;

struct Dataset {
  unsigned capa;
  unsigned length;
  unsigned next;
  Value **arr;
  friend bool operator==(Dataset const &, Dataset const &);
  friend bool operator!=(Dataset const &, Dataset const &);
};

class Value
{
  enum valueType {
    Bool = 0, Int, Float, Time, String, Error, Retention, Dataset,
    LastValueType
  } valueType;
  union V {
    V() {};
    ~V() {};
    bool Bool;
    int Int;
    double Float;
    double Time;
    QString String;
    struct Error Error;
    struct Retention Retention;
    struct Dataset Dataset;
  } v;
public:
  // construct uninitialized
  Value();
  // Construct from an OCaml value
  Value(value);
  // Copy constructor
  Value(const Value&);

  Value& operator=(const Value&);
  bool is_initialized() const;

  QString toQString() const;

  friend bool operator==(Value const &, Value const &);
  friend bool operator!=(Value const &, Value const &);
};

std::ostream &operator<<(std::ostream &, Value const &);
QDebug operator<<(QDebug, Value const &);

};

Q_DECLARE_METATYPE(conf::Value);

#endif
