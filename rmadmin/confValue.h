#ifndef CONFVALUE_H_190504
#define CONFVALUE_H_190504
#include <iostream>
#include <QString>
extern "C" {
#  include <caml/mlvalues.h>
}

namespace conf {

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
    struct Error {
      double time;
      unsigned cmd_id;
      std::string msg;
    } Error;
    struct Retention {
      double duration;
      double period;
    } Retention;
    struct Dataset {
      unsigned capa;
      unsigned length;
      unsigned next;
      Value **arr;
    } Dataset;
  } v;
public:
  // construct uninitialized
  Value();
  // Construct from an OCaml value
  Value(value);
  // Copy constructor
  Value(const Value&);

  Value& operator=(const Value&);
  bool is_initialized();

  QString toQString() const;
};

std::ostream &operator<<(std::ostream &, Value const &);

};

#endif
