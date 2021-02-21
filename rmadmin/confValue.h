#ifndef CONFVALUE_H_190504
#define CONFVALUE_H_190504
#include <cassert>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <QColor>
#include <QCoreApplication>
#include <QPair>
#include <QSet>
#include <QString>
#include <QMetaType>
#include <QObject>
extern "C" {
# include <caml/mlvalues.h>
// Defined by OCaml mlvalues but conflicting with further Qt includes:
# undef alloc
# undef flush
}
#include "CompiledFunctionInfo.h"
#include "CompiledProgramParam.h"
#include "confRCEntry.h"
#include "confWorkerRef.h"
#include "RamenValue.h"

struct AlertInfo;
class AtomicWidget;
class QPainter;
struct RamenType;

namespace conf {

// Must match RamenSync.Value.t!
enum ValueType {
  ErrorType,
  WorkerType,
  RetentionType,
  TimeRangeType,
  TuplesType, // A serialized batch of tuples, not a VTuple
  RamenValueType,  // For RamenTypes.value
  TargetConfigType,
  SourceInfoType,
  RuntimeStatsType,
  ReplayType,
  ReplayerType,
  AlertType,
  ReplayRequestType,
  OutputSpecsType,
  DashWidgetType,
  AlertingContactType,
  NotificationType,
  DeliveryStatusType,
  IncidentLogType,
  InhibitionType,
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
  // Can depend on the key to adapt string representation:
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
  QString const toQString(std::string const &) const override;
  value toOCamlValue() const override;
  bool operator==(Value const &) const override;
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

  QString const toQString(std::string const &) const override;
  AtomicWidget *editorWidget(
    std::string const &key, QWidget *parent = nullptr) const override;
  bool operator==(Value const &) const override;
};

struct Retention : public Value
{
  /* TODO: duration is now an expression, we need general support for
   * expressions that we might get from dessser serializer at some point */
  double period;

  Retention() : period(0.) {};
  Retention(value const);
  Retention(Retention const &other) : Value(other.valueType) {
    period = other.period;
  }

  QString const toQString(std::string const &) const override;
  value toOCamlValue() const override;
  bool operator==(Value const &) const override;
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
  QString const toQString(std::string const &) const override;
  AtomicWidget *editorWidget(
    std::string const &key, QWidget *parent = nullptr) const override;
  value toOCamlValue() const override;
  bool operator==(Value const &) const override;
};

/* Tuples of ramen _values_, not to be confused with RamenTypeTuple which is a tuple
 * of ramen _types_.
 * Actually, nothing mandates [val] to be a tuple; in the future, when I/O are not
 * restricted to tuples, rename this Tuple into just RamenValue? */
struct Tuples : public Value
{
  struct Tuple {
    unsigned skipped;
    uint32_t const *bytes;
    size_t num_words; // words of 4 bytes

    Tuple(unsigned, unsigned char const *, size_t);

    // returned value belongs to caller:
    RamenValue *unserialize(std::shared_ptr<RamenType const>) const;
    bool operator==(Tuple const &) const;
  };

  std::vector<Tuple> tuples;

  Tuples() : Value(TuplesType) {}
  Tuples(value);

  QString const toQString(std::string const &) const override;
  value toOCamlValue() const override;
  bool operator==(Value const &) const override;
};

struct RamenValueValue : public Value
{
  std::shared_ptr<RamenValue const> v;

  // Takes ownership of v_
  RamenValueValue(RamenValue *v_) :
    Value(RamenValueType), v(v_) {}

  RamenValueValue(std::shared_ptr<RamenValue const> v_) :
    Value(RamenValueType), v(v_) {}

  QString const toQString(std::string const &k) const override {
    return v->toQString(k);
  }

  value toOCamlValue() const override;

  AtomicWidget *editorWidget(
    std::string const &key, QWidget *parent = nullptr) const override;

  bool isNull() const override { return v->isNull(); }

  bool operator==(Value const &) const override;
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

  bool operator==(Value const &) const override;
  QString const toQString(std::string const &) const override;
  AtomicWidget *editorWidget(
    std::string const &key, QWidget *parent = nullptr) const override;

  bool isInfo() const { return errMsg.isEmpty(); }
  bool isError() const { return !isInfo(); }
};

struct TargetConfig : public Value
{
  std::map<std::string const, std::shared_ptr<RCEntry>> entries;

  TargetConfig() : Value(TargetConfigType) {}
  TargetConfig(value);
  // Deep copy the passed object:
  TargetConfig(TargetConfig const &);

  value toOCamlValue() const override;

  bool operator==(Value const &) const override;
  QString const toQString(std::string const &) const override;

  AtomicWidget *editorWidget(
    std::string const &key, QWidget *parent = nullptr) const override;

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
  uint64_t totInputTuples, totSelectedTuples, totFilteredTuples, totOutputTuples;
  uint64_t totFullBytes, totFullBytesSamples; // Measure the full size of output tuples
  uint64_t curGroups, maxGroups;
  uint64_t totInputBytes, totOutputBytes;
  double totWaitIn, totWaitOut;
  uint64_t totFiringNotifs, totExtinguishedNotifs;
  double totCpu;
  uint64_t curRam, maxRam;

  RuntimeStats() : Value(RuntimeStatsType) {};
  RuntimeStats(value);

  QString const toQString(std::string const &) const override;
  AtomicWidget *editorWidget(
    std::string const &key, QWidget *parent = nullptr) const override;
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

  QString const toQString(std::string const &) const override;
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

  value toOCamlValue() const override;
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;
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

  value toOCamlValue() const override;
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;
};

struct OutputSpecs : public Value
{
  // TODO
  OutputSpecs() : Value(OutputSpecsType) {}
  OutputSpecs(value);
};

struct DashWidget : public Value
{
  DashWidget() : Value(DashWidgetType) {}
  virtual value toOCamlValue() const override = 0;
};

struct DashWidgetText : public DashWidget
{
  QString text;

  DashWidgetText() : DashWidget() {}
  DashWidgetText(value);
  DashWidgetText(QString const &);
  value toOCamlValue() const override;
  AtomicWidget *editorWidget(
    std::string const &key, QWidget *parent = nullptr) const override;
  bool operator==(Value const &) const override;
};

struct DashWidgetChart : public DashWidget
{
  QString title;

  enum ChartType {
    Plot
  } type;

  struct Axis {
    bool left;
    bool forceZero;
    enum Scale { Linear, Logarithmic } scale;
    Axis(bool l, bool f, Scale s)
      : left(l), forceZero(f), scale(s) {}
    value toOCamlValue() const;
    bool operator==(Axis const &) const;
    bool operator!=(Axis const &) const;
  };

  struct Column {
    std::string name;
    enum Representation {
      Unused, Independent, Stacked, StackCentered
    } representation;
    int axisNum;
    std::vector<std::string> factors;
    QColor color;

    Column(std::string const cn, Representation r, int an, QColor c)
      : name(cn), representation(r), axisNum(an), color(c) {}
    /* Create with a random color associated with this fully-qualified
     * field name */
    Column(std::string const &program, std::string const &function,
           std::string const &field);
    Column(value);
    value toOCamlValue() const;
    static QString const nameOfRepresentation(Representation);
    bool operator==(Column const &) const;
    bool operator!=(Column const &) const;
  };

  struct Source {
    std::string site, program, function;
    QString name; // used to order the sources
    bool visible;
    std::list<Column> fields;

    Source(std::string const sn, std::string const pn, std::string const fn,
           bool visible_ = true)
      : site(sn), program(pn), function(fn), visible(visible_) {}
    Source(value);
    value toOCamlValue() const;
    bool operator==(Source const &) const;
    bool operator!=(Source const &) const;
  };

  std::vector<Axis> axes;
  std::vector<Source> sources;  // ordered by name

  DashWidgetChart() : DashWidget() {}
  DashWidgetChart(value);
  // Create an empty chart for this function:
  DashWidgetChart(
    std::string const sn, std::string const pn, std::string const fn);
  value toOCamlValue() const override;
  AtomicWidget *editorWidget(
    std::string const &key, QWidget *parent = nullptr) const override;
  bool operator==(Value const &) const override;
};


struct AlertingContact : public Value
{
  double timeout;

  AlertingContact() : Value(AlertingContactType) {}
  AlertingContact(double timeout_)
    : Value(AlertingContactType), timeout(timeout_) {}
  QString const toQString(std::string const &) const override;
};

struct AlertingContactIgnore : public AlertingContact
{
  QString cmd;

  AlertingContactIgnore() : AlertingContact() {}
  AlertingContactIgnore(double);
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;
};

struct AlertingContactExec : public AlertingContact
{
  QString cmd;

  AlertingContactExec() : AlertingContact() {}
  AlertingContactExec(double, value);
  AlertingContactExec(double, QString const &);
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;
};

struct AlertingContactSysLog : public AlertingContact
{
  QString msg;

  AlertingContactSysLog() : AlertingContact() {}
  AlertingContactSysLog(double, value);
  AlertingContactSysLog(double, QString const &);
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;
};

struct AlertingContactSqlite : public AlertingContact
{
  QString file;
  QString insert;
  QString create;

  AlertingContactSqlite() : AlertingContact() {}
  AlertingContactSqlite(double, value);
  AlertingContactSqlite(double, QString const &, QString const &, QString const &);
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;
};

struct AlertingContactKafka : public AlertingContact
{
  QSet<QPair<QString, QString>> options;
  QString topic;
  unsigned partition;
  QString text;

  AlertingContactKafka() : AlertingContact() {}
  AlertingContactKafka(double, value);
  AlertingContactKafka(
    double,
    QSet<QPair<QString, QString>> const &, QString const &, unsigned,
    QString const &);
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;
};

struct Notification : public Value
{
  QString site;
  QString worker;
  bool test;
  double sentTime;
  std::optional<double> eventTime;
  QString name;
  bool firing;
  double certainty;
  double debounce;
  double timeout;
  QSet<QPair<QString, QString>> parameters;

  Notification() : Value(NotificationType) {}
  Notification(value);
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;
};

struct DeliveryStatus : public Value
{
  enum Status {
    StartToBeSent,
    StartToBeSentThenStopped,
    StartSent,
    StartAcked,
    StopToBeSent,
    StopSent,
    NUM_STATUS
  } status;

  DeliveryStatus() : Value(DeliveryStatusType) {}
  DeliveryStatus(value);
  DeliveryStatus(enum Status);
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;
};

struct IncidentLog : public Value
{
  enum LogTag {
    TagNewNotification, TagOutcry, TagAck, TagStop, TagCancel
  };

  enum Outcome {
    Duplicate, Inhibited, STFU, Escalate
  };

  QString text;
  enum TickKind {
    TickStart, TickDup, TickInhibited, TickOutcry, TickAck, TickStop, TickCancel
  } tickKind;

  IncidentLog() : Value(IncidentLogType) {}
  IncidentLog(value);
  QString const toQString(std::string const &) const override;
  bool operator==(Value const &) const override;

  void paintTick(QPainter *, qreal width, qreal x, qreal y0, qreal y1) const;
};

struct Inhibition : public Value
{
  /* TODO */
  Inhibition() : Value(InhibitionType) {}
  Inhibition(value);
  bool operator==(Value const &) const override;
};

QDebug operator<<(QDebug debug, DashWidgetChart::Source const &);

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
Q_DECLARE_METATYPE(conf::Tuples);

#endif
