#include <cassert>
#include <string.h>
#include <cstdlib>
#include <cstring>
#include <QtWidgets>
#include <QString>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
}
#include "misc.h"
#include "confRamenValue.h"
#include "confValue.h"
#include "confWorkerRole.h"
#include "confWorkerRef.h"
#include "confRCEntryParam.h"

namespace conf {

static QString const stringOfValueType(ValueType valueType)
{
  static QString const stringOfValueTypes[] = {
    [ErrorType] = "ErrorType",
    [WorkerType] = "WorkerType",
    [RetentionType] = "RetentionType",
    [TimeRangeType] = "TimeRangeType",
    [TupleType] = "TupleType",
    [RamenValueType] = "RamenValueType",
    [TargetConfigType] = "TargetConfigType",
    [SourceInfoType] = "SourceInfoType",
    [RuntimeStatsType] = "RuntimeStatsType",
    [ReplayType] = "ReplayType",
    [ReplayerType] = "ReplayerType",
    [AlertType] = "AlertType",
    [LastValueType] = "LastValueType",
  };
  assert((size_t)valueType < SIZEOF_ARRAY(stringOfValueTypes));
  return stringOfValueTypes[(size_t)valueType];
}

Value::Value()
{
  valueType = LastValueType;
}

Value::Value(ValueType valueType_) : valueType(valueType_) {}

Value::~Value() {}

QString Value::toQString() const
{
  return QString("TODO: toQString for ") + stringOfValueType(valueType);
}

value Value::toOCamlValue() const
{
  assert(!"Don't know how to convert from a base Value");
}

bool Value::operator==(Value const &other) const
{
  return valueType == other.valueType;
}

bool Value::operator!=(Value const &other) const
{
  return !operator==(other);
}

Value *valueOfOCaml(value v_)
{
  CAMLparam1(v_);
  CAMLlocal4(tmp1_, tmp2_, tmp3_, tmp4_);
  assert(Is_block(v_));
  ValueType valueType = (ValueType)Tag_val(v_);
  Value *ret = nullptr;
  switch (valueType) {
    case ErrorType:
      ret = new Error(
        Double_val(Field(v_, 0)),
        (unsigned)Int_val(Field(v_, 1)),
        String_val(Field(v_, 2)));
      break;
    case WorkerType:
      {
        v_ = Field(v_, 0);
        assert(Wosize_val(v_) == 12);
        Worker *w = new Worker(
          Bool_val(Field(v_, 0)), // enabled
          Bool_val(Field(v_, 1)), // debug
          Double_val(Field(v_, 2)), // reportPeriod
          String_val(Field(v_, 3)), // srcPath
          String_val(Field(v_, 4)), // worker_signature
          String_val(Field(v_, 5)), // bin_signature
          Bool_val(Field(v_, 6)), // used
          WorkerRole::ofOCamlValue(Field(v_, 9)));
        // Add the params:
        for (tmp1_ = Field(v_, 7); Is_block(tmp1_); tmp1_ = Field(tmp1_, 1)) {
          RCEntryParam *p = new RCEntryParam(
            String_val(Field(tmp1_, 0)), // name
            std::shared_ptr<conf::RamenValue const>(RamenValue::ofOCaml(Field(tmp1_, 1))));
          w->params.push_back(p);
        }
        // Add the parents:
        for (tmp1_ = Field(v_, 10); Is_block(tmp1_); tmp1_ = Field(tmp1_, 1)) {
          WorkerRef *p = WorkerRef::ofOCamlValue(Field(tmp1_, 0));
          w->parent_refs.push_back(p);
        }
        // TODO: add everything else
        ret = w;
      }
      break;
    case RetentionType:
      v_ = Field(v_, 0);
      assert(Tag_val(v_) == Double_array_tag);
      ret = new Retention(
        Double_field(v_, 0),
        Double_field(v_, 1));
      break;
    case TimeRangeType:
      {
        std::vector<std::pair<double, double>> range;
        range.reserve(5);
        // TODO: read that value
        range.push_back(std::pair<double, double>(1., 2.));
        ret = new TimeRange(range);
      }
      break;
    case TupleType:
      ret = new Tuple(
        // If the value is <0 it must have wrapped around in the OCaml side.
        Int_val(Field(v_, 0)),
        Bytes_val(Field(v_, 1)),
        caml_string_length(Field(v_, 1)));
      break;
    case RamenValueType:
      ret = new RamenValueValue(
        RamenValue::ofOCaml(Field(v_, 0)));
      break;
    case TargetConfigType:
      {
        TargetConfig *targetConfig = new TargetConfig();
        ret = targetConfig;
        // Iter over the cons cells:
        for (tmp1_ = Field(v_, 0); Is_block(tmp1_); tmp1_ = Field(tmp1_, 1)) {
          tmp2_ = Field(tmp1_, 0);  // the pname * rc_entry pair
          assert(Is_block(tmp2_));
          tmp3_ = Field(tmp2_, 1);  // the rc_entry
          assert(Is_block(tmp3_)); // XXX
          assert(Is_block(Field(tmp2_, 0)));
          RCEntry *rcEntry = new RCEntry(
            String_val(Field(tmp2_, 0)),  // pname
            Bool_val(Field(tmp3_, 0)),  // enabled
            Bool_val(Field(tmp3_, 1)),  // debug
            Double_val(Field(tmp3_, 2)),  // report_period
            String_val(Field(tmp3_, 4)),  // src_file
            String_val(Field(tmp3_, 5)),  // on_site (as a string)
            Bool_val(Field(tmp3_, 6)));  // automatic
          // Add the params:
          for (tmp2_ = Field(tmp3_, 3); Is_block(tmp2_); tmp2_ = Field(tmp2_, 1)) {
            tmp4_ = Field(tmp2_, 0);  // the name * value
            RCEntryParam *param = new RCEntryParam(
              String_val(Field(tmp4_, 0)),  // name
              std::shared_ptr<RamenValue const>(RamenValue::ofOCaml(Field(tmp4_, 1)))); // value
            rcEntry->addParam(param);
          }
          targetConfig->addEntry(rcEntry);
        }
      }
      break;
    case SourceInfoType:
      {
        v_ = Field(v_, 0);
        QString md5(String_val(Field(v_, 0)));
        v_ = Field(v_, 1);
        switch (Tag_val(v_)) {
          case 0: // CompiledSourceInfo
            {
              v_ = Field(v_, 0);
              // `Some expression` for the running condition
              bool hasRunCond = Is_block(Field(v_, 1));
              SourceInfo *sourceInfo = new SourceInfo(md5, hasRunCond);
              ret = sourceInfo;
              // Iter over the cons cells of the RamenTuple.params:
              for (tmp1_ = Field(v_, 0); Is_block(tmp1_); tmp1_ = Field(tmp1_, 1)) {
                tmp2_ = Field(tmp1_, 0);  // the RamenTuple.param
                tmp3_ = Field(tmp2_, 0);  // the ptyp field
                CompiledProgramParam *p =
                  new CompiledProgramParam(
                    String_val(Field(tmp3_, 0)),  // name
                    String_val(Field(tmp3_, 3)),  // doc
                    std::shared_ptr<conf::RamenValue const>(RamenValue::ofOCaml(Field(tmp2_, 1)))); // value
                sourceInfo->addParam(p);
              }
              // Iter over the cons cells of the function_info:
              for (tmp1_ = Field(v_, 2); Is_block(tmp1_); tmp1_ = Field(tmp1_, 1)) {
                tmp2_ = Field(tmp1_, 0);  // the function_info
                tmp3_ = Field(tmp2_, 1);  // the (optional) retention
                Retention *retention = nullptr;
                if (Is_block(tmp3_)) {
                  tmp3_ = Field(tmp3_, 0);
                  assert(Tag_val(tmp3_) == Double_array_tag);
                  retention =
                    new Retention(Double_field(tmp3_, 0), Double_field(tmp3_, 1));
                }
                CompiledFunctionInfo *f =
                  new CompiledFunctionInfo(
                    String_val(Field(tmp2_, 0)),  // name
                    retention,
                    Bool_val(Field(tmp2_, 2)),    // is_lazy
                    String_val(Field(tmp2_, 3)),  // doc
                    /* We skip the operation, too hard to parse. We'd need a proxy
                     * in the OCaml receiver that turns it into a string. */
                    String_val(Field(tmp2_, 5))); // signature
                sourceInfo->addInfo(f);
              }
            }
            break;
          case 1: // FailedSourceInfo
            {
            v_ = Field(v_, 0);
            assert(Tag_val(Field(v_, 0)) == String_tag);
            SourceInfo *sourceInfo = new SourceInfo(md5, QString(String_val(Field(v_, 0))));
            std::cout << "info is error!! '" << sourceInfo->errMsg.toStdString() << "'" << std::endl;
            ret = sourceInfo;
            }
            break;
          default:
            assert(!"Not a detail_source_info?!");
        }
      }
      break;
    case RuntimeStatsType:
      ret = new RuntimeStats(Field(v_, 0));
      break;
    case ReplayType:
      ret = new Replay(Field(v_, 0));
      break;
    case ReplayerType:
      ret = new Replayer(Field(v_, 0));
      break;
    case AlertType:
      ret = new Alert(Field(v_, 0));
    case LastValueType:
    default:
      assert(!"Tag_val(v_) <= LastValueType");
  }
  CAMLreturnT(Value *, ret);
}

Value *valueOfQString(ValueType vt, QString const &s)
{
  bool ok = true;
  Value *ret = nullptr;
  switch (vt) {
    case ErrorType:
    case WorkerType:
    case RetentionType:
    case TimeRangeType:
    case TupleType:
    case TargetConfigType:
    case SourceInfoType:
    case RuntimeStatsType:
    case ReplayType:
    case ReplayerType:
    case AlertType:
      assert(!"TODO: valueOfQString for exotic types");
      break;
    case RamenValueType:
      assert(!"Cannot convert to RamenValue without a RamenType");
    case LastValueType:
      assert(!"Invalid conf::ValueType");
  }
  if (! ret)
    assert(!"Tag_val(v_) <= LastValueType");
  if (! ok)
    std::cerr << "Cannot convert " << s.toStdString() << " into a value" << std::endl;
  return ret;
}

Error::Error(double time_, unsigned cmdId_, std::string const &msg_) :
  Value(ErrorType), time(time_), cmdId(cmdId_), msg(msg_) {}

Error::Error() : Error(0., 0, "") {}

Error::~Error() {}

QString Error::toQString() const
{
  (void)time;
  return QString::fromStdString(msg); // TODO: prepend with time etc.
}

value Error::toOCamlValue() const
{
  assert(!"Don't know how to convert from an Error");
}

bool Error::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Error const &o = static_cast<Error const &>(other);
  return cmdId == o.cmdId;
}

Worker::Worker(bool enabled_, bool debug_, double reportPeriod_, QString const &srcPath_, QString const &worker_sign_, QString const &bin_sign_, bool used_, WorkerRole *role_) :
  Value(WorkerType),
  enabled(enabled_),
  debug(debug_),
  reportPeriod(reportPeriod_),
  srcPath(srcPath_),
  worker_sign(worker_sign_),
  bin_sign(bin_sign_),
  used(used_),
  role(role_) {}

Worker::Worker() : Worker(false, false, 0., "", "", "", false, nullptr) {}

Worker::~Worker()
{
  if (role) delete role;
  for (auto p : parent_refs) {
    delete p;
  }
}

bool Worker::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Worker const &o = static_cast<Worker const &>(other);
  return enabled == o.enabled && debug == o.debug && reportPeriod == o.reportPeriod && srcPath == o.srcPath && worker_sign == o.worker_sign && bin_sign == o.bin_sign && used == o.used && role == o.role;
}

Retention::Retention(double duration_, double period_) :
  Value(RetentionType), duration(duration_), period(period_) {}

Retention::Retention() : Retention(0., 0.) {}

Retention::~Retention() {}

bool Retention::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Retention const &o = static_cast<Retention const &>(other);
  return duration == o.duration && period == o.period;
}

QString Retention::toQString() const
{
  return QString("duration: ").
         append(QString::number(duration)).
         append(", period: ").
         append(QString::number(period));
}

value Retention::toOCamlValue() const
{
  assert(!"Don't know how to convert form a Retention");
}

TimeRange::TimeRange(std::vector<std::pair<double, double>> const &range_) :
  Value(TimeRangeType), range(range_) {}

TimeRange::TimeRange() : TimeRange(std::vector<std::pair<double,double>>()) {}

TimeRange::~TimeRange() {}

QString TimeRange::toQString() const
{
  return QString("TODO: TimeRange to string");
}

value TimeRange::toOCamlValue() const
{
  assert(!"Don't know how to convert from a TimeRange");
}

bool TimeRange::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  TimeRange const &o = static_cast<TimeRange const &>(other);
  return range == o.range;
}

Tuple::Tuple(unsigned skipped_, unsigned char const *bytes_, size_t size_) :
  Value(TupleType), skipped(skipped_), size(size_)
{
  if (bytes_) {
    bytes = new char[size];
    memcpy((void *)bytes, (void *)bytes_, size);
  } else {
    assert(size == 0);
    bytes = nullptr;
  }
}

Tuple::Tuple() : Tuple(0, nullptr, 0) {}

Tuple::~Tuple()
{
  if (bytes) delete[](bytes);
}

QString Tuple::toQString() const
{
  return QString::number(size) + QString(" bytes");
}

ser::Value *Tuple::unserialize(std::shared_ptr<RamenType const> type) const
{
  uint32_t const *start = (uint32_t const *)bytes;  // TODO: check alignment
  uint32_t const *max = (uint32_t const *)(bytes + size); // TODO: idem
  ser::Value *v = ser::unserialize(type, start, max, true);
  assert(start == max);
  return v;
}

value Tuple::toOCamlValue() const
{
  assert(!"Don't know how to convert from an Tuple");
}

bool Tuple::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Tuple const &o = static_cast<Tuple const &>(other);
  return size == o.size && 0 == memcmp(bytes, o.bytes, size);
}

bool RamenValueValue::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  RamenValueValue const &o = static_cast<RamenValueValue const &>(other);
  return value == o.value;
}

SourceInfo::SourceInfo() : SourceInfo(QString(), QString()) {}

SourceInfo::~SourceInfo()
{
  while (! params.isEmpty()) {
    delete (params.takeLast());
  }
  while (! infos.isEmpty()) {
    delete (infos.takeLast());
  }
}

bool SourceInfo::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  SourceInfo const &o = static_cast<SourceInfo const &>(other);
  if (md5 != o.md5) return false;
  if (isInfo()) {
    return o.isInfo() && params == o.params && infos == o.infos;
  } else {
    return !o.isInfo() && errMsg == o.errMsg;
  }
}

TargetConfig::~TargetConfig()
{
  for (auto entry : entries) {
    delete entry.second;
  }
}

value TargetConfig::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal4(ret, lst, cons, pair);
  ret = caml_alloc(1, TargetConfigType);
  // Then a list of program_name * rc_enrtry:
  lst = Val_emptylist;  // Ala Val_int(0)
  for (auto const it : entries) {
    RCEntry const *entry = it.second;
    pair = caml_alloc_tuple(2);
    Store_field(pair, 0, caml_copy_string(it.first.c_str()));
    Store_field(pair, 1, entry->toOCamlValue());
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 1, lst);
    Store_field(cons, 0, pair);
    lst = cons;
  }
  Store_field(ret, 0, lst);
  CAMLreturn(ret);
}


bool TargetConfig::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  TargetConfig const &o = static_cast<TargetConfig const &>(other);
  return entries == o.entries;
}

RuntimeStats::RuntimeStats(value v_)
{
  assert(24 == Wosize_val(v_));
  statsTime = Double_val(Field(v_, 0));
  firstStartup = Double_val(Field(v_, 1));
  lastStartup = Double_val(Field(v_, 2));
  minEventTime = Is_block(Field(v_, 3)) ?
    std::optional<double>(Double_val(Field(Field(v_, 3), 0))) :
    std::optional<double>(),
  maxEventTime = Is_block(Field(v_, 4)) ?
    std::optional<double>(Double_val(Field(Field(v_, 4), 0))) :
    std::optional<double>(),
  firstInput = Is_block(Field(v_, 5)) ?
    std::optional<double>(Double_val(Field(Field(v_, 5), 0))) :
    std::optional<double>(),
  lastInput = Is_block(Field(v_, 6)) ?
    std::optional<double>(Double_val(Field(Field(v_, 6), 0))) :
    std::optional<double>(),
  firstOutput = Is_block(Field(v_, 7)) ?
    std::optional<double>(Double_val(Field(Field(v_, 7), 0))) :
    std::optional<double>(),
  lastOutput = Is_block(Field(v_, 8)) ?
    std::optional<double>(Double_val(Field(Field(v_, 8), 0))) :
    std::optional<double>(),
  totInputTuples = Long_val(Field(v_, 9));
  totSelectedTuples = Long_val(Field(v_, 10));
  totOutputTuples = Long_val(Field(v_, 11));
  totFullBytes = *(uint64_t *)Data_custom_val(Field(v_, 12));
  totFullBytesSamples = *(uint64_t *)Data_custom_val(Field(v_, 13));
  curGroups = Long_val(Field(v_, 14));
  totInputBytes = *(uint64_t *)Data_custom_val(Field(v_, 15));
  totOutputBytes = *(uint64_t *)Data_custom_val(Field(v_, 16));
  totWaitIn = Double_val(Field(v_, 17));
  totWaitOut = Double_val(Field(v_, 18));
  totFiringNotifs = Long_val(Field(v_, 19));
  totExtinguishedNotifs = Long_val(Field(v_, 20));
  totCpu = Double_val(Field(v_, 21));
  curRam = *(uint64_t *)Data_custom_val(Field(v_, 22));
  maxRam = *(uint64_t *)Data_custom_val(Field(v_, 23));
}

Replay::Replay(value v_)
{
  assert(9 == Wosize_val(v_));
  // wtv, not used anywhere in the GUI for now
}

Replayer::Replayer(value v_)
{
  assert(6 == Wosize_val(v_));
  // wtv, not used anywhere in the GUI for now
}

Alert::Alert(value v_)
{
  assert(1 == Wosize_val(v_)); // v1
  // wtv, not used anywhere in the GUI for now
}

std::ostream &operator<<(std::ostream &os, Value const &v)
{
  os << v.toQString().toStdString();
  return os;
}

};
