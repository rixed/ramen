#ifndef CONFVALUE_H_190504
#define CONFVALUE_H_190504
#include <cassert>
#include <string>
#include <memory>
#include <optional>
#include <QCoreApplication>
#include <QString>
#include <QMetaType>
#include <QObject>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "CompiledProgramParam.h"
#include "CompiledFunctionInfo.h"
#include "RamenValue.h"
#include "confRCEntry.h"
#include "confWorkerRef.h"

struct RamenType;
class AtomicWidget;
struct AlertInfo;

namespace conf {

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
  AlertType,
  ReplayRequestType,
  OutputSpecsType
};

QString const stringOfValueType(ValueType);

class Value
{
public:
  // FIXME: necessary to have that type?
  ValueType valueType;
  // Construct uninitialized
  Value(ValueType);
  Value() : Value(ErrorType) {} // wtv
  virtual ~Value() {};

  operator QString() const { return toQString(std::string()); }
  // TODO: get rid of this, replace by the above
  virtual QString const toQString(std::string const &) const;

  virtual value toOCamlValue() const;
  /* Generic editor that can be overwritten/specialized/tuned
   * according to the key. By default a read-only label. */
  virtual AtomicWidget *editorWidget(std::string const &key, QWidget *parent = nullptr) const;
  /* Tells if the value is the ramen value Null (used to quickly skip nulls */
  virtual bool isNull() const { return false; }
  virtual bool operator==(Value const &) const;
  bool operator!=(Value const &) const;
};

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
  QString const toQString(std::string const &) const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
};

struct WorkerRole;

struct Worker : public Value
{
  bool enabled;
  bool debug;
  bool used;
  double reportPeriod;
  QString cwd;
  QString workerSign;
  QString binSign;
  WorkerRole *role;
  std::list<RCEntryParam *> params; // Params are owned
  std::list<WorkerRef *> parent_refs; // WorkerRef are owned

  Worker(value);

  Worker() :
    Value(WorkerType), enabled(false), debug(false), used(false),
    reportPeriod(0.), role(nullptr)
  {}

  ~Worker();

  QString const toQString(std::string const &) const;
  AtomicWidget *editorWidget(std::string const &key, QWidget *parent = nullptr) const;
  bool operator==(Value const &) const;
};

struct Retention : public Value
{
  double duration;
  double period;

  Retention();
  Retention(double, double);
  Retention(Retention const &other) : Value(other.valueType) {
    duration = other.duration;
    period = other.period;
  }

  QString const toQString(std::string const &) const;
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
    bool isValid() const { return t1 < t2 && t1 >= 0; }
  };

  /* Beware that ranges can overlaps: */
  std::vector<Range> range;

  TimeRange() : Value(TimeRangeType) {}
  TimeRange(std::vector<Range> const &range_) :
    Value(TimeRangeType), range(range_) {}
  TimeRange(value);

  bool isEmpty() const { return range.empty(); }
  double length() const;
  QString const toQString(std::string const &) const;
  AtomicWidget *editorWidget(std::string const &key, QWidget *parent = nullptr) const;
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
  uint32_t const *bytes;
  size_t num_words; // words of 4 bytes

  Tuple();
  ~Tuple();
  Tuple(unsigned, unsigned char const *, size_t);

  QString const toQString(std::string const &) const;
  value toOCamlValue() const;
  bool operator==(Value const &) const;
  // returned value belongs to caller:
  RamenValue *unserialize(std::shared_ptr<RamenType const>) const;
};

struct RamenValueValue : public Value
{
  std::shared_ptr<RamenValue const> v;

  // Takes ownership of v_
  RamenValueValue(RamenValue *v_) :
    Value(RamenValueType), v(v_) {}
  RamenValueValue(std::shared_ptr<RamenValue const> v_) :
    Value(RamenValueType), v(v_) {}

  QString const toQString(std::string const &k) const {
    return v->toQString(k);
  }

  value toOCamlValue() const;
  AtomicWidget *editorWidget(std::string const &key, QWidget *parent = nullptr) const;

  bool isNull() const { return v->isNull(); }

  bool operator==(Value const &) const;
};

// Read-only (pre)compilation output for a program:
struct SourceInfo : public Value
{
  QString src_ext;
  QStringList md5s;
  // If this is not empty then everything else is irrelevant.
  QString errMsg;
  std::vector<std::shared_ptr<CompiledProgramParam>> params;
  std::vector<std::shared_ptr<CompiledFunctionInfo>> infos;

  SourceInfo() {}
  SourceInfo(value);

  bool operator==(Value const &) const;
  QString const toQString(std::string const &) const;
  AtomicWidget *editorWidget(std::string const &key, QWidget *parent = nullptr) const;

  bool isInfo() const { return errMsg.isEmpty(); }
  bool isError() const { return !isInfo(); }
};

struct TargetConfig : public Value
{
  std::map<std::string const, std::shared_ptr<RCEntry>> entries;

  TargetConfig() : Value(TargetConfigType) {}
  TargetConfig(value);

  value toOCamlValue() const;

  bool operator==(Value const &) const;
  QString const toQString(std::string const &) const;

  AtomicWidget *editorWidget(std::string const &key, QWidget *parent = nullptr) const;

  // Takes ownership
  void addEntry(std::shared_ptr<RCEntry> entry) {
    entries[entry->programName] = entry;
  }
};

struct RuntimeStats : public Value
{
  double statsTime, firstStartup, lastStartup;
  std::optional<double> minEventTime, maxEventTime;
  std::optional<double> firstInput, lastInput;
  std::optional<double> firstOutput, lastOutput;
  uint64_t totInputTuples, totSelectedTuples, totOutputTuples;
  uint64_t totFullBytes, totFullBytesSamples; // Measure the full size of output tuples
  uint64_t curGroups;
  uint64_t totInputBytes, totOutputBytes;
  double totWaitIn, totWaitOut;
  uint64_t totFiringNotifs, totExtinguishedNotifs;
  double totCpu;
  uint64_t curRam, maxRam;

  RuntimeStats() : Value(RuntimeStatsType) {};
  RuntimeStats(value);

  QString const toQString(std::string const &) const;
  AtomicWidget *editorWidget(std::string const &key, QWidget *parent = nullptr) const;
};

struct SiteFq
{
  QString site;
  QString fq;
  SiteFq() {}
  SiteFq(value);
  QString const toQString() const { return site + QString(":") + fq; }
};

struct Replay : public Value
{
  int channel;
  SiteFq target;
  double since;
  double until;
  QString final_ringbuf_file;
  std::vector<SiteFq> sources;
  std::vector<std::pair<SiteFq, SiteFq>> links;
  double timeout_date;

  Replay() : Value(ReplayType) {}
  Replay(value);

  QString const toQString(std::string const &) const;
};

struct Replayer : public Value
{
  // wtv
  Replayer() : Value(ReplayerType) {}
  Replayer(value);
};

struct Alert : public Value
{
  AlertInfo *info;

  Alert() : Value(AlertType) {}
  Alert(value);
  Alert(std::unique_ptr<AlertInfo> info_) :
    Value(AlertType), info(info_.release()) {}

  ~Alert();

  value toOCamlValue() const;
  QString const toQString(std::string const &) const;
  bool operator==(Value const &) const;
};

struct ReplayRequest : public Value
{
  std::string site, program, function;
  // TODO: fieldMask
  double since, until;
  bool explain;
  std::string respKey;

  ReplayRequest(
    std::string const &site,
    std::string const &program,
    std::string const &function,
    double since, double until,
    bool explain,
    std::string const &respKey);
  ReplayRequest(value);

  value toOCamlValue() const;
  QString const toQString(std::string const &) const;
  bool operator==(Value const &) const;
};

struct OutputSpecs : public Value
{
  // TODO
  OutputSpecs() : Value(OutputSpecsType) {}
  OutputSpecs(value);
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
