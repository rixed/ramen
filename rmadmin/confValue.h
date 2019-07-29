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
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
}
#include "CompiledProgramParam.h"
#include "CompiledFunctionInfo.h"
#include "RamenValue.h"
#include "confRCEntry.h"
#include "confWorkerRef.h"

struct RamenType;
class AtomicWidget;

namespace conf {

class Key;

// Must match RamenSync.Value.t!
enum ValueType {
  ErrorType,
  WorkerType,
  RetentionType,
  TimeRangeType,
  TupleType, // A serialized tuple, not a VTuple
  RamenValueType,  // For RamenTypes.value
  TargetConfigType,
  SourceInfoType,
  RuntimeStatsType,
  ReplayType,
  ReplayerType,
  AlertType
};

class Value
{
  // FIXME: necessary to have that type?
  ValueType valueType;
public:
  // Construct uninitialized
  Value(ValueType);
  Value() : Value(ErrorType) {} // wtv
  virtual ~Value() {};

  virtual QString const toQString(Key const &) const;
  virtual value toOCamlValue() const;
  /* Generic editor that can be overwritten/specialized/tuned
   * according to the key. By default a read-only label. */
  virtual AtomicWidget *editorWidget(Key const &key, QWidget *parent = nullptr) const;
  virtual bool operator==(Value const &) const;
  bool operator!=(Value const &) const;
};

std::ostream &operator<<(std::ostream &, Value const &);

// Construct from an OCaml value
Value *valueOfOCaml(value);
// Construct from a QString
Value *valueOfQString(ValueType, QString const &);


struct Error : public Value
{
  double time;
  unsigned cmdId;
  std::string msg;

  Error(double time_, unsigned cmdId_, std::string const &msg_) :
    Value(ErrorType), time(time_), cmdId(cmdId_), msg(msg_) {}
  Error() : Error(0., 0, "") {}
  QString const toQString(Key const &) const;
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
  QString const workerSign;
  QString const binSign;
  bool used;
  WorkerRole *role;
  std::list<RCEntryParam *> params; // Params are owned
  std::list<WorkerRef *> parent_refs; // WorkerRef are owned

  Worker();
  ~Worker();
  Worker(bool enabled, bool debug, double reportPeriod, QString const &srcPath, QString const &worker_sign, QString const &bin_sign, bool used, WorkerRole *role);
  QString const toQString(Key const &) const;
  bool operator==(Value const &) const;
};

struct Retention : public Value
{
  double duration;
  double period;

  Retention();
  ~Retention();
  Retention(double, double);
  QString const toQString(Key const &) const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct TimeRange : public Value
{
  struct Range {
    double t1, t2;
    bool openEnded;
    Range(double t1_, double t2_, bool openEnded_) :
      t1(t1_), t2(t2_), openEnded(openEnded_) {}
    bool operator==(Range const &other) const {
      return t1 == other.t1 && t2 == other.t2 && openEnded == other.openEnded;
    }
  };
  std::vector<Range> range;

  TimeRange() : Value(TimeRangeType) {}
  TimeRange(std::vector<Range> const &range_) :
    Value(TimeRangeType), range(range_) {}
  TimeRange(value);
  QString const toQString(Key const &) const;
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
  QString const toQString(Key const &) const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
  // returned value belongs to caller:
  RamenValue *unserialize(std::shared_ptr<RamenType const>) const;
};

struct RamenValueValue : public Value
{
  std::shared_ptr<RamenValue> v;

  // Takes ownership of v_
  RamenValueValue(RamenValue *v_) :
    Value(RamenValueType), v(v_) {}

  QString const toQString(Key const &k) const { return v->toQString(k); }

  value toOCamlValue() const;
  AtomicWidget *editorWidget(Key const &key, QWidget *parent = nullptr) const;
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

  SourceInfo(QString md5_, QString errMsg_) :
    Value(SourceInfoType), md5(md5_), errMsg(errMsg_) {}
  SourceInfo(QString md5_, bool hasRunCondition_) :
    Value(SourceInfoType), md5(md5_), hasRunCondition(hasRunCondition_) {}
  SourceInfo() : SourceInfo(QString(), QString()) {}
  ~SourceInfo();

  bool operator==(Value const &) const;
  QString const toQString(Key const &) const;

  bool isInfo() const { return errMsg.isEmpty(); }
  bool isError() const { return !isInfo(); }

  // Takes ownership
  void addParam(CompiledProgramParam *p) { params.append(p); }
  void addInfo(CompiledFunctionInfo *i) { infos.append(i); }
};

struct TargetConfig : public Value
{
  std::map<std::string const, RCEntry *> entries;

  TargetConfig() : Value(TargetConfigType) {}
  TargetConfig(value);
  ~TargetConfig();

  value toOCamlValue() const;

  bool operator==(Value const &) const;
  QString const toQString(Key const &) const;

  AtomicWidget *editorWidget(Key const &key, QWidget *parent = nullptr) const;

  // Takes ownership
  void addEntry(RCEntry *entry) {
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

  RuntimeStats() : Value(RuntimeStatsType) {};
  RuntimeStats(value);
  QString const toQString(Key const &) const;
};

struct Replay : public Value
{
  // wtv
  Replay() : Value(ReplayType) {}
  Replay(value);
};

struct Replayer : public Value
{
  // wtv
  Replayer() : Value(ReplayerType) {}
  Replayer(value);
};

struct Alert : public Value
{
  // wtv
  Alert() : Value(AlertType) {}
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

#endif
