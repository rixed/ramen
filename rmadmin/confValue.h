#ifndef CONFVALUE_H_190504
#define CONFVALUE_H_190504
#include <iostream>
#include <cassert>
#include <QCoreApplication>
#include <QString>
#include <QMetaType>
#include <QObject>
extern "C" {
# include <caml/mlvalues.h>
}
#include "serValue.h"
#include "CompiledProgramParam.h"
#include "CompiledFunctionInfo.h"
#include "confRamenValue.h"
#include "confRCEntry.h"
#include "confWorkerRef.h"

struct RamenType;

namespace conf {

enum ValueType {
  ErrorType, WorkerType, RetentionType, TimeRangeType,
  // Beware: conf::TupleType is the type of a tuple received for tailling
  // (ie number of skipped tuples + serialized value), while
  // ser::TupleType is the type of a RamenValue (equivalent to T.TTuple).
  TupleType,
  RamenValueType,  // For RamenTypes.value
  TargetConfigType,
  SourceInfoType,
  RuntimeStatsType,
  ReplayType,
  ReplayerType,
  AlertType,
  LastValueType
};

class Value
{
  ValueType valueType;
public:
  // Construct uninitialized
  Value();
  Value(ValueType);
  virtual ~Value();

  virtual QString toQString() const;
  virtual value toOCamlValue() const;
  virtual bool operator==(Value const &) const;
  bool operator!=(Value const &) const;
};

std::ostream &operator<<(std::ostream &, Value const &);

// Construct from an OCaml value
Value *valueOfOCaml(value);
// Construct from a QString
Value *valueOfQString(conf::ValueType, QString const &);


struct Error : public Value
{
  double time;
  unsigned cmdId;
  std::string msg;
  Error();
  ~Error();
  Error(double, unsigned, std::string const &);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct WorkerRole;

struct Worker : public Value
{
  bool enabled;
  bool debug;
  double reportPeriod;
  QString const srcPath;
  QString const worker_sign;
  QString const bin_sign;
  bool used;
  WorkerRole *role;
  std::list<RCEntryParam *> params; // Params are owned
  std::list<WorkerRef *> parent_refs; // WorkerRef are owned

  Worker();
  ~Worker();
  Worker(bool enabled, bool debug, double reportPeriod, QString const &srcPath, QString const &worker_sign, QString const &bin_sign, bool used, WorkerRole *role);
  bool operator==(Value const &) const;
};

struct Retention : public Value
{
  double duration;
  double period;
  Retention();
  ~Retention();
  Retention(double, double);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct TimeRange : public Value
{
  std::vector<std::pair<double, double>> range;
  TimeRange();
  ~TimeRange();
  TimeRange(std::vector<std::pair<double, double>> const &);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

/* Tuple of ramen _values_, not to be confused with RamenTypeTuple which is a tuple
 * of ramen _types_.
 * Actually, nothing mandates [val] to be a tuple; in the future, when I/O are not
 * restricted to tuples, rename this Tuple into just RamenValue? */
struct Tuple : public Value
{
  unsigned skipped;
  char const *bytes;
  size_t size;
  Tuple();
  ~Tuple();
  Tuple(unsigned, unsigned char const *, size_t);
  QString toQString() const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
  // returned value belongs to caller:
  ser::Value *unserialize(std::shared_ptr<RamenType const>) const;
};

// FIXME: make this a template over conf::RamenValue
struct RamenValueValue : public Value
{
  std::shared_ptr<conf::RamenValue> value;

  RamenValueValue(RamenValue *value_) :
    Value(RamenValueType), value(value_) {}

  QString toQString() const { return value->toQString(); }
  bool operator==(Value const &) const;
};

// Read-only (pre)compilation output for a program:
struct SourceInfo : public Value
{
  QString md5;
  // If this is not empty then everything else is irrelevant.
  QString errMsg;
  QList<CompiledProgramParam *> params;
  bool hasRunCondition;
  QList<CompiledFunctionInfo *> infos;

  SourceInfo();
  SourceInfo(QString md5_, QString errMsg_) :
    Value(SourceInfoType), md5(md5_), errMsg(errMsg_) {}
  SourceInfo(QString md5_, bool hasRunCondition_) :
    Value(SourceInfoType), md5(md5_), hasRunCondition(hasRunCondition_) {}
  ~SourceInfo();

  bool operator==(Value const &) const;

  bool isInfo() const { return errMsg.isEmpty(); }
  bool isError() const { return !isInfo(); }

  // Takes ownership
  void addParam(CompiledProgramParam *p) { params.append(p); }
  void addInfo(CompiledFunctionInfo *i) { infos.append(i); }
};

struct TargetConfig : public Value
{
  std::map<std::string const, RCEntry *> entries;

  TargetConfig() {}
  ~TargetConfig();

  value toOCamlValue() const;

  bool operator==(Value const &) const;

  // Takes ownership
  void addEntry(RCEntry *entry)
  {
    entries[entry->programName] = entry;
  }
};

struct RuntimeStats : public Value
{
  double statsTime, firstStartup, lastStartup;
  std::optional<double> minEventTime, maxEventTime;
  std::optional<double> firstInput, lastInput;
  std::optional<double> firstOutput, lastOutput;
  unsigned totInputTuples, totSelectedTuples, totOutputTuples;
  size_t totFullBytes, totFullBytesSamples; // Measure the full size of output tuples
  unsigned curGroups;
  size_t totInputBytes, totOutputBytes;
  double totWaitIn, totWaitOut;
  unsigned totFiringNotifs, totExtinguishedNotifs;
  double totCpu;
  size_t curRam, maxRam;

  RuntimeStats() {};
  RuntimeStats(value);
};

struct Replay : public Value
{
  // wtv
  Replay() {}
  Replay(value);
};

struct Replayer : public Value
{
  // wtv
  Replayer() {}
  Replayer(value);
};

struct Alert : public Value
{
  // wtv
  Alert() {}
  Alert(value);
};

};

Q_DECLARE_METATYPE(std::shared_ptr<conf::Value const>);
Q_DECLARE_METATYPE(conf::Error);
Q_DECLARE_METATYPE(conf::SourceInfo);
Q_DECLARE_METATYPE(conf::RuntimeStats);
Q_DECLARE_METATYPE(conf::Replay);
Q_DECLARE_METATYPE(conf::Replayer);
Q_DECLARE_METATYPE(conf::Alert);
Q_DECLARE_METATYPE(conf::Worker);
Q_DECLARE_METATYPE(conf::TimeRange);
Q_DECLARE_METATYPE(conf::Retention);
Q_DECLARE_METATYPE(conf::Tuple);

// Defined by OCaml columnName but conflicting with further Qt includes:
#undef alloc

#endif
