#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>
#include <QDebug>
#include <QPainter>
#include <QString>
#include <QtGlobal>
#include <QtWidgets>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
# undef alloc
# undef flush
}

#include "alerting/tools.h"
#include "colorOfString.h"
#include "misc.h"
#include "RamenValue.h"
#include "RamenType.h"
#include "chart/TimeChartEditWidget.h"
#include "confWorkerRole.h"
#include "confWorkerRef.h"
#include "confRCEntryParam.h"
#include "dashboard/DashboardWidgetText.h"
#include "TargetConfigEditor.h"
#include "TimeRangeViewer.h"
#include "RuntimeStatsViewer.h"
#include "WorkerViewer.h"
#include "SourceInfoViewer.h"
#include "AlertInfo.h"
#include "KLabel.h"

#include "confValue.h"

static bool const verbose(false);

namespace conf {

QString const stringOfValueType(ValueType valueType)
{
  switch (valueType) {
    case ErrorType: return QString("ErrorType");
    case WorkerType: return QString("WorkerType");
    case RetentionType: return QString("RetentionType");
    case TimeRangeType: return QString("TimeRangeType");
    case TuplesType: return QString("TuplesType");
    case RamenValueType: return QString("RamenValueType");
    case TargetConfigType: return QString("TargetConfigType");
    case SourceInfoType: return QString("SourceInfoType");
    case RuntimeStatsType: return QString("RuntimeStatsType");
    case ReplayType: return QString("ReplayType");
    case ReplayerType: return QString("ReplayerType");
    case AlertType: return QString("AlertType");
    case ReplayRequestType: return QString("ReplayRequestType");
    case OutputSpecsType: return QString("OutputSpecsType");
    case DashWidgetType: return QString("DashWidgetType");
    case AlertingContactType: return QString("AlertingContactType");
    case NotificationType: return QString("NotificationType");
    case DeliveryStatusType: return QString("DeliveryStatusType");
    case IncidentLogType: return QString("IncidentLogType");
    case InhibitionType: return QString("InhibitionType");
  };
  assert(!"invalid valueType");
  return QString();
}

Value::Value(ValueType valueType_) : valueType(valueType_) {}

QString const Value::toQString(std::string const &) const
{
  return QString("TODO: toQString for ") + stringOfValueType(valueType);
}

value Value::toOCamlValue() const
{
  assert(!"Don't know how to convert from a base Value");
}

AtomicWidget *Value::editorWidget(std::string const &key, QWidget *parent) const
{
  KLabel *editor = new KLabel(parent);
  editor->setKey(key);
  return editor;
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
      ret = new Worker(Field(v_, 0));
      break;
    case RetentionType:
      ret = new Retention(Field(v_, 0));
      break;
    case TimeRangeType:
      ret = new TimeRange(Field(v_, 0));
      break;
    case TuplesType:
      ret = new Tuples(Field(v_, 0));
      break;
    case RamenValueType:
      ret = new RamenValueValue(
        RamenValue::ofOCaml(Field(v_, 0)));
      break;
    case TargetConfigType:
      ret = new TargetConfig(Field(v_, 0));
      break;
    case SourceInfoType:
      ret = new SourceInfo(Field(v_, 0));
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
      break;
    case ReplayRequestType:
      ret = new ReplayRequest(Field(v_, 0));
      break;
    case OutputSpecsType:
      ret = new OutputSpecs(Field(v_, 0));
      break;
    case DashWidgetType:
      v_ = Field(v_, 0);
      assert(Is_block(v_));
      if (Tag_val(v_) == 0) { /* Text */
        ret = new DashWidgetText(v_);
      } else {  /* Chart */
        assert(Tag_val(v_) == 1);
        ret = new DashWidgetChart(v_);
      }
      break;
    case AlertingContactType:
      {
        v_ = Field(v_, 0);
        assert(Is_block(v_));
        assert(Wosize_val(v_) == 2);
        double timeout { Double_val(Field(v_, 1)) };
        v_ = Field(v_, 0);  // via
        if (Is_block(v_)) {
          switch (Tag_val(v_)) {
            case 0:  // ViaExec
              ret = new AlertingContactExec(timeout, v_);
              break;
            case 1:  // ViaSysLog
              ret = new AlertingContactSysLog(timeout, v_);
              break;
            case 2:  // ViaSqlite
              ret = new AlertingContactSqlite(timeout, v_);
              break;
            case 3:  // ViaKafka
              ret = new AlertingContactKafka(timeout, v_);
              break;
            default:
              qFatal("Invalid AlertingContactType tag");
              break;
          }
        } else {
          assert(Int_val(v_) == 0); // Ignore
          ret = new AlertingContactIgnore(timeout);
        }
      }
      break;
    case NotificationType:
      ret = new Notification(Field(v_, 0));
      break;
    case DeliveryStatusType:
      ret = new DeliveryStatus(Field(v_, 0));
      break;
    case IncidentLogType:
      ret = new IncidentLog(Field(v_, 0));
      break;
    case InhibitionType:
      ret = new Inhibition(Field(v_, 0));
      break;
  }
  if (! ret) {
    assert(!"Tag_val(v_) <= ReplayRequestType");
  }
  CAMLreturnT(Value *, ret);
}

Value *valueOfQString(ValueType vt, QString const &)
{
  Value *ret = nullptr;
  switch (vt) {
    case ErrorType:
    case WorkerType:
    case RetentionType:
    case TimeRangeType:
    case TuplesType:
    case TargetConfigType:
    case SourceInfoType:
    case RuntimeStatsType:
    case ReplayType:
    case ReplayerType:
    case AlertType:
    case ReplayRequestType:
    case OutputSpecsType:
    case DashWidgetType:
    case AlertingContactType:
    case NotificationType:
    case DeliveryStatusType:
    case IncidentLogType:
    case InhibitionType:
      assert(!"TODO: valueOfQString for exotic types");
      break;
    case RamenValueType:
      assert(!"Cannot convert to RamenValue without a RamenType");
  }
  assert(ret && "Tag_val(v_) <= ReplayRequestType");
  return ret;
}

QString const Error::toQString(std::string const &) const
{
  return
    stringOfDate(time) + QString(": #") + QString::number(cmdId) + QString(": ") +
    (msg.length() > 0 ? QString::fromStdString(msg) : QString("Ok"));
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

Worker::Worker(value v_) : Value(WorkerType)
{
  CAMLparam1(v_);
  CAMLlocal2(cons_, p_);

  assert(Wosize_val(v_) == 12);
  enabled = Bool_val(Field(v_, 0));
  debug = Bool_val(Field(v_, 1));
  reportPeriod = Double_val(Field(v_, 2));
  cwd = String_val(Field(v_, 3));
  workerSign = String_val(Field(v_, 4));
  binSign = String_val(Field(v_, 5));
  used = Bool_val(Field(v_, 6));
  // Add the params:
  for (cons_ = Field(v_, 7); Is_block(cons_); cons_ = Field(cons_, 1)) {
    p_ = Field(cons_, 0);
    RCEntryParam *p = new RCEntryParam(
      String_val(Field(p_, 0)), // name
      std::shared_ptr<RamenValue const>(RamenValue::ofOCaml(Field(p_, 1))));
    params.push_back(p);
  }
  // Field 8 is envvars: TODO
  role = WorkerRole::ofOCamlValue(Field(v_, 9));
  // Field 10: parents:
  if (Is_block(Field(v_, 10))) {  // Some
    for (cons_ = Field(Field(v_, 10), 0); Is_block(cons_); cons_ = Field(cons_, 1)) {
      WorkerRef *p = WorkerRef::ofOCamlValue(Field(cons_, 0));
      parent_refs.push_back(p);
    }
  }  // else None

  // TODO: field 11 is children
  CAMLreturn0;
}

Worker::~Worker()
{
  if (role) delete role;
  for (auto p : parent_refs) {
    delete p;
  }
  for (auto p : params) {
    delete p;
  }
}

bool Worker::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Worker const &o = static_cast<Worker const &>(other);
  return enabled == o.enabled && debug == o.debug &&
         reportPeriod == o.reportPeriod && cwd == o.cwd &&
         workerSign == o.workerSign && binSign == o.binSign &&
         used == o.used && role == o.role;
}

QString const Worker::toQString(std::string const &) const
{
  QString s;
  s += QString("Status: ") + (enabled ? QString("enabled") : QString("disabled"));
  s += QString(", Role: ") + role->toQString();
  return s;
}

AtomicWidget *Worker::editorWidget(std::string const &key, QWidget *parent) const
{
  WorkerViewer *editor = new WorkerViewer(parent);
  editor->setKey(key);
  return editor;
}

Retention::Retention(value const v_) : Value(RetentionType)
{
  assert(Is_block(v_));
  assert(Tag_val(v_) == 0); // record
  // Field 0 is the expression for the duration
  period = Double_val(Field(v_, 1));
}

bool Retention::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Retention const &o = static_cast<Retention const &>(other);
  return period == o.period;
}

QString const Retention::toQString(std::string const &) const
{
  return QString("for SOME DURATION (TODO)").
         append(", every ").append(stringOfDuration(period));
}

value Retention::toOCamlValue() const
{
  assert(!"Don't know how to convert form a Retention");
}

TimeRange::TimeRange(value v_) : Value(TimeRangeType)
{
  CAMLparam1(v_);
  while (Is_block(v_)) {
    range.emplace_back(Double_val(Field(Field(v_, 0), 0)),
                       Double_val(Field(Field(v_, 0), 1)),
                       Bool_val(Field(Field(v_, 0), 2)));
    Range const &r = range[range.size() - 1];
    if (! r.isValid()) {
      qWarning() << "TimeRange: skipping invalid range"
                 << stringOfDate(r.t1) << "..." << stringOfDate(r.t2);
      range.pop_back();
    }
    v_ = Field(v_, 1);
  }
  CAMLreturn0;
}

QString const TimeRange::toQString(std::string const &) const
{
  if (0 == range.size()) return QString("empty");

  double duration = 0;
  for (auto const &p : range) duration += p.t2 - p.t1;

  double const since = range[0].t1;
  double const until = range[range.size()-1].t2;
  bool const openEnded = range[range.size()-1].openEnded;

  QString s = stringOfDuration(duration);
  s += QString(" since ")
     + stringOfDate(since)
     + QString(" until ")
     + (openEnded ? QString("at least ") : QString(""))
     + stringOfDate(until);
  return s;
}

value TimeRange::toOCamlValue() const
{
  assert(!"Don't know how to convert from a TimeRange");
}

AtomicWidget *TimeRange::editorWidget(std::string const &key, QWidget *parent) const
{
  TimeRangeViewer *editor = new TimeRangeViewer(parent);
  editor->setKey(key);
  return editor;
}

double TimeRange::length() const
{
  /* FIXME: Deal with overlaps: to begin with, keep it sorted. */
  double res = 0;
  for (Range const &r : range)
    res += r.t2 - r.t1;
  return res;
}

bool TimeRange::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  TimeRange const &o = static_cast<TimeRange const &>(other);
  return range == o.range;
}

Tuples::Tuple::Tuple(unsigned skipped_, unsigned char const *bytes_, size_t size) :
  skipped(skipped_), num_words(size / 4)
{
  assert(0 == (size & 3));
  assert(bytes_);

  if (verbose)
    qDebug() << "New tuple of" << num_words << "words";

  bytes = new uint32_t[num_words];
  memcpy((void *)bytes, (void *)bytes_, size);
}

RamenValue *Tuples::Tuple::unserialize(std::shared_ptr<RamenType const> type) const
{
  uint32_t const *start = bytes;
  uint32_t const *max = bytes + num_words;
  RamenValue *v = type->vtyp->unserialize(start, max, true);
  assert(start == max);
  return v;
}

bool Tuples::Tuple::operator==(Tuples::Tuple const &o) const
{
  return num_words == o.num_words &&
         0 == memcmp(bytes, o.bytes, num_words * sizeof(uint32_t));
}

Tuples::Tuples(value v_) : Value(TuplesType)
{
  CAMLparam1(v_);
  CAMLlocal1(t_);
  assert(Is_block(v_)); // supposed to be an array

  size_t const numTuples(Wosize_val(v_));
  tuples.reserve(numTuples);
  for (size_t i = 0; i < numTuples; i++) {
    t_ = Field(v_, i);
    tuples.emplace_back(
      // If the value is <0 it must have wrapped around in the OCaml side.
      Int_val(Field(t_, 0)), // skipped
      Bytes_val(Field(t_, 1)), // serialized values
      caml_string_length(Field(t_, 1)));
  }

  CAMLreturn0;
}

QString const Tuples::toQString(std::string const &) const
{
  return QString::number(tuples.size()) + QString(" tuples");
}

value Tuples::toOCamlValue() const
{
  assert(!"Don't know how to convert from an Tuple");
}

bool Tuples::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Tuples const &o = static_cast<Tuples const &>(other);
  if (tuples.size() != o.tuples.size()) return false;
  for (size_t i = 0; i < tuples.size(); i++) {
    if (! tuples[i].operator==(o.tuples[i])) return false;
  }
  return true;
}

// This _does_ alloc on the OCaml heap
value RamenValueValue::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, RamenValueType);
  Store_field(ret, 0, v->toOCamlValue());
  CAMLreturn(ret);
}

AtomicWidget *RamenValueValue::editorWidget(std::string const &key, QWidget *parent) const
{
  return v->editorWidget(key, parent);
}

bool RamenValueValue::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  RamenValueValue const &o = static_cast<RamenValueValue const &>(other);
  return *v == *o.v;
}

SourceInfo::SourceInfo(value v_) : Value(SourceInfoType)
{
  CAMLparam1(v_);
  CAMLlocal4(md5_, cons_, param_, func_);
  assert(3 == Wosize_val(v_));
  src_ext = String_val(Field(v_, 0));
  for (md5_ = Field(v_, 1); Is_block(md5_); md5_ = Field(md5_, 1)) {
    md5s.append(String_val(Field(md5_, 0)));
  }
  v_ = Field(v_, 2);
  switch (Tag_val(v_)) {
    case 0: // CompiledSourceInfo
      {
        v_ = Field(v_, 0);
        assert(4 == Wosize_val(v_));

        // Iter over the cons cells of the RamenTuple.params:
        params.reserve(10);
        for (cons_ = Field(v_, 0); Is_block(cons_); cons_ = Field(cons_, 1)) {
          param_ = Field(cons_, 0);  // the RamenTuple.param
          params.emplace_back(std::make_shared<CompiledProgramParam>(param_));
        }
        // TODO: condition (or nuke it)
        // TODO: globals
        // Iter over the cons cells of the function_info:
        infos.reserve(10);
        for (cons_ = Field(v_, 3); Is_block(cons_); cons_ = Field(cons_, 1)) {
          func_ = Field(cons_, 0);  // the function_info
          infos.emplace_back(std::make_shared<CompiledFunctionInfo>(func_));
        }
        if (verbose)
          qDebug() << "info is a program with" << params.size() << "params"
                   << "and" << infos.size() << "functions";
      }
      break;
    case 1: // FailedSourceInfo
      {
        v_ = Field(v_, 0);
        assert(2 == Wosize_val(v_)); // err_msg and depends_on
        assert(Tag_val(Field(v_, 0)) == String_tag);
        errMsg = QString(String_val(Field(v_, 0)));
        if (verbose)
          qDebug() << "info is compil failure:" << errMsg;
      }
      break;
    default:
      assert(!"Not a detail_source_info?!");
  }
  CAMLreturn0;
}

bool SourceInfo::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  SourceInfo const &o = static_cast<SourceInfo const &>(other);
  /* Notice: QList::operator!= does the right thing :
   * (as long as the lists are ordered, though (FIXME: use a QSet)) */
  if (md5s != o.md5s) return false;
  if (isInfo()) {
    return o.isInfo(); // in theory, compare params
  } else {
    return !o.isInfo() && errMsg == o.errMsg;
  }
}

QString const SourceInfo::toQString(std::string const &) const
{
  if (errMsg.length() > 0) return errMsg;

  QString s("");
  for (auto const &info : infos) {
    if (s.length() > 0) s += QString(", ");
    s += info->name;
  }

  return QString("Compiled functions from " + src_ext + ": ") + s;
}

AtomicWidget *SourceInfo::editorWidget(std::string const &key, QWidget *parent) const
{
  SourceInfoViewer *editor = new SourceInfoViewer(parent);
  editor->setKey(key);
  return editor;
}

TargetConfig::TargetConfig(value v_)
  : Value(TargetConfigType)
{
  CAMLparam1(v_);
  // Iter over the cons cells:
  while (Is_block(v_)) {
    value pair = Field(v_, 0);  // the pname * rc_entry pair

    assert(Is_block(pair));
    value rce_ = Field(pair, 1);  // the rc_entry
    assert(Is_block(rce_));
    assert(Is_block(Field(pair, 0)));
    std::shared_ptr<RCEntry> rcEntry = std::make_shared<RCEntry>(
      String_val(Field(pair, 0)),  // pname
      Bool_val(Field(rce_, 0)),  // enabled
      Bool_val(Field(rce_, 1)),  // debug
      Double_val(Field(rce_, 2)),  // report_period
      String_val(Field(rce_, 3)),  // cwd
      String_val(Field(rce_, 5)),  // on_site (as a string)
      Bool_val(Field(rce_, 6)));  // automatic
    for (value params_ = Field(rce_, 4); Is_block(params_); params_ = Field(params_, 1)) {
      value param_ = Field(params_, 0);  // the name * value
      RCEntryParam *param = new RCEntryParam(
        String_val(Field(param_, 0)),  // name
        std::shared_ptr<RamenValue const>(RamenValue::ofOCaml(Field(param_, 1)))); // value
      rcEntry->addParam(param);
    }
    addEntry(rcEntry);

    v_ = Field(v_, 1);
  }
  CAMLreturn0;
}

TargetConfig::TargetConfig(TargetConfig const &o)
  : Value(TargetConfigType)
{
  // The entries themselves are shareable:
  for (std::pair<std::string const, std::shared_ptr<RCEntry>> const &p : o.entries)
    addEntry(p.second);
}

// This _does_ alloc on the OCaml heap
value TargetConfig::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal4(ret, lst, cons, pair);
  checkInOCamlThread();
  // Then a list of program_name * rc_enrtry:
  lst = Val_emptylist;  // Ala Val_int(0)
  for (auto const &it : entries) {
    pair = caml_alloc_tuple(2);
    Store_field(pair, 0, caml_copy_string(it.first.c_str()));
    Store_field(pair, 1, it.second->toOCamlValue());
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 1, lst);
    Store_field(cons, 0, pair);
    lst = cons;
  }
  ret = caml_alloc(1, TargetConfigType);
  Store_field(ret, 0, lst);
  CAMLreturn(ret);
}

AtomicWidget *TargetConfig::editorWidget(std::string const &key, QWidget *parent) const
{
  TargetConfigEditor *editor = new TargetConfigEditor(parent);
  editor->setKey(key);
  return editor;
}

bool TargetConfig::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  TargetConfig const &o = static_cast<TargetConfig const &>(other);

  /* To compare the map by RCEntry values (rather than pointer equality)
   * we have to reimplement map comparison here.
   * First, check there is no more keys in other.entries: */
  if (o.entries.size() != entries.size()) return false;

  /* Then, check that all our keys are present with the same value
   * in other: */
  for (auto const &mapEntry : entries) {
    auto other_entry_it = o.entries.find(mapEntry.first);
    if (other_entry_it == o.entries.end()) return false;
    if (*other_entry_it->second != *mapEntry.second) return false;
  }

  return true;
}

QString const TargetConfig::toQString(std::string const &) const
{
  if (0 == entries.size()) return QString("empty");
  QString s;
  for (auto const &rce : entries) {
    if (s.length() > 0) s += QString("\n");
    s += QString::fromStdString(rce.first);
    s += QString(" on ") + QString::fromStdString(rce.second->onSite);
  }
  return s;
}

RuntimeStats::RuntimeStats(value v_) : Value(RuntimeStatsType)
{
  CAMLparam1(v_);
# define Uint64_val(x) *(uint64_t *)Data_custom_val(x)
  assert(26 == Wosize_val(v_));
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
  totInputTuples = Uint64_val(Field(v_, 9));
  totSelectedTuples = Uint64_val(Field(v_, 10));
  totFilteredTuples = Uint64_val(Field(v_, 11));
  totOutputTuples = Uint64_val(Field(v_, 12));
  totFullBytes = Uint64_val(Field(v_, 13));
  totFullBytesSamples = Uint64_val(Field(v_, 14));
  curGroups = Uint64_val(Field(v_, 15));
  maxGroups = Uint64_val(Field(v_, 16));
  totInputBytes = Uint64_val(Field(v_, 17));
  totOutputBytes = Uint64_val(Field(v_, 18));
  totWaitIn = Double_val(Field(v_, 19));
  totWaitOut = Double_val(Field(v_, 20));
  totFiringNotifs = Uint64_val(Field(v_, 21));
  totExtinguishedNotifs = Uint64_val(Field(v_, 22));
  totCpu = Double_val(Field(v_, 23));
  curRam = Uint64_val(Field(v_, 24));
  maxRam = Uint64_val(Field(v_, 25));
  CAMLreturn0;
}

QString const RuntimeStats::toQString(std::string const &) const
{
  QString s("Stats-time: ");
  s += stringOfDate(statsTime);
  if (maxEventTime)
    s += QString(", front-time: ") + stringOfDate(*maxEventTime);
  return s;
}

AtomicWidget *RuntimeStats::editorWidget(std::string const &key, QWidget *parent) const
{
  RuntimeStatsViewer *editor = new RuntimeStatsViewer(parent);
  editor->setKey(key);
  return editor;
}

SiteFq::SiteFq(value v_)
{
  CAMLparam1(v_);
  assert(2 == Wosize_val(v_));
  site = String_val(Field(v_, 0));
  fq = String_val(Field(v_, 1));

  CAMLreturn0;
}

Replay::Replay(value v_) :
  Value(ReplayType)
{
  CAMLparam1(v_);
  assert(9 == Wosize_val(v_));
  channel = Long_val(Field(v_, 0));
  target = SiteFq(Field(v_, 1));
  since = Double_val(Field(v_, 3));
  until = Double_val(Field(v_, 4));
  final_ringbuf_file = String_val(Field(v_, 5));
  for (value src_ = Field(v_, 6); Is_block(src_); src_ = Field(src_, 1)) {
    sources.emplace_back(SiteFq(Field(src_, 0)));
  }
  for (value src_ = Field(v_, 7); Is_block(src_); src_ = Field(src_, 1)) {
    links.emplace_back(
      SiteFq(Field(Field(src_, 0), 0)),
      SiteFq(Field(Field(src_, 0), 1)));
  }
  timeout_date = Double_val(Field(v_, 8));
  CAMLreturn0;
}

QString const Replay::toQString(std::string const &) const
{
  QString s("Channel: ");
  s += QString::number(channel);
  s += QString(", to ") + target.toQString();
  s += QString(", since ") + stringOfDate(since);
  s += QString(", until +") + stringOfDuration(until - since);
  s += QString(", involving ") + QString::number(links.size()) + QString(" links.");
  return s;
}

Replayer::Replayer(value v_) : Value(ReplayerType)
{
  CAMLparam1(v_);
  assert(6 == Wosize_val(v_));
  // wtv, not used anywhere in the GUI for now
  CAMLreturn0;
}

Alert::Alert(value v_) : Value(AlertType)
{
  CAMLparam1(v_);
  info = new AlertInfo(v_);
  CAMLreturn0;
}

Alert::~Alert()
{
  delete info;
}

value Alert::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(1, AlertType);
  Store_field(ret, 0, info->toOCamlValue());
  CAMLreturn(ret);
}

QString const Alert::toQString(std::string const &) const
{
  if (info) {
    return info->toQString();
  } else {
    return QString("no info");
  }
}

bool Alert::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Alert const &o = static_cast<Alert const &>(other);

  if (info == o.info) return true;
  if (info == nullptr || o.info == nullptr) return false;
  return *info == *o.info;
}

static void site_fq(
  std::string *site, std::string *program, std::string *function, value v_)
{
  assert(2 == Wosize_val(v_));

  *site = String_val(Field(v_, 0));
  if (verbose)
    qDebug() << "ReplayRequest::ReplayRequest: site=" << QString::fromStdString(*site);
  std::string const fq = String_val(Field(v_, 1));
  if (verbose)
    qDebug() << "ReplayRequest::ReplayRequest: fq=" << QString::fromStdString(fq);
  size_t lst = fq.rfind('/');
  *program = fq.substr(0, lst);
  if (verbose)
    qDebug() << "ReplayRequest::ReplayRequest: program=" << QString::fromStdString(*program);
  *function = fq.substr(lst + 1);
  if (verbose)
    qDebug() << "ReplayRequest::ReplayRequest: function=" << QString::fromStdString(*function);
}

static value alloc_site_fq(
  std::string const &site, std::string const &program, std::string const &function)
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc_tuple(2);
  Store_field(ret, 0, caml_copy_string(site.c_str()));
  std::string const fq(program + "/" + function);
  Store_field(ret, 1, caml_copy_string(fq.c_str()));
  CAMLreturn(ret);
}

ReplayRequest::ReplayRequest(
  std::string const &site_,
  std::string const &program_,
  std::string const &function_,
  double since_, double until_,
  bool explain_,
  std::string const &respKey_)
  : Value(ReplayRequestType),
    site(site_), program(program_), function(function_),
    since(since_), until(until_), explain(explain_), respKey(respKey_) {}

ReplayRequest::ReplayRequest(value v_) : Value(ReplayRequestType)
{
  CAMLparam1(v_);
  assert(5 == Wosize_val(v_));
  site_fq(&site, &program, &function, Field(v_, 0));
  since = Double_val(Field(v_, 1));
  until = Double_val(Field(v_, 2));
  explain = Bool_val(Field(v_, 3));
  respKey = String_val(Field(v_, 4));
  CAMLreturn0;
}

value ReplayRequest::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, request);
  checkInOCamlThread();
  ret = caml_alloc(1, ReplayRequestType);
  request = caml_alloc_tuple(5);
  Store_field(request, 0, alloc_site_fq(site, program, function));
  Store_field(request, 1, caml_copy_double(since));
  Store_field(request, 2, caml_copy_double(until));
  Store_field(request, 3, Val_bool(explain));
  Store_field(request, 4, caml_copy_string(respKey.c_str()));
  Store_field(ret, 0, request);
  CAMLreturn(ret);
}

QString const ReplayRequest::toQString(std::string const &) const
{
  return QString::fromStdString(
    (explain ? "{ EXPLAIN for site_fq=" : "{ site_fq=") +
    site + ":" + program + "/" + function +
    " resp_key=" + respKey + " }");
}

bool ReplayRequest::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  ReplayRequest const &o = static_cast<ReplayRequest const &>(other);

  return respKey == o.respKey &&
         since == o.since && until == o.until && explain == o.explain &&
         site == o.site && program == o.program && function == o.function;
}

OutputSpecs::OutputSpecs(value v_) : Value(OutputSpecsType)
{
  CAMLparam1(v_);
  /* Make it something else than a hashtbl! For instance, individual entries.
   * Would also save on locks. */
  assert(Is_block(v_));
  // TODO
  CAMLreturn0;
}

DashWidgetText::DashWidgetText(value v_) : DashWidget()
{
  CAMLparam1(v_);
  assert(Wosize_val(v_) == 1);
  text = QString(String_val(Field(v_, 0)));
  CAMLreturn0;
}

DashWidgetText::DashWidgetText(QString const &text_)
  : DashWidget(),
    text(text_)
{}

// This _does_ alloc on the OCaml heap
value DashWidgetText::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, widget);
  checkInOCamlThread();
  widget = caml_alloc(1, 0);
  ret = caml_alloc(1, DashWidgetType);
  Store_field(widget, 0, caml_copy_string(text.toStdString().c_str()));
  Store_field(ret, 0, widget);
  CAMLreturn(ret);
}

AtomicWidget *DashWidgetText::editorWidget(std::string const &key, QWidget *parent) const
{
  DashboardWidgetText *editor = new DashboardWidgetText(nullptr, parent);
  editor->setKey(key);
  return editor;
}

bool DashWidgetText::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  /* The above just guarantee that we have a DashWidget, but not
   * that it is a DashWidgetText, thus the dynamic_cast: */
  try {
    DashWidgetText const &o =
      dynamic_cast<DashWidgetText const &>(other);
    return text == o.text;
  } catch (std::bad_cast const &) {
    return false;
  }
}

DashWidgetChart::Source::Source(value v_)
{
  CAMLparam1(v_);
  CAMLlocal1(a_);

  assert(3 == Wosize_val(v_));

  site_fq(&site, &program, &function, Field(v_, 0));
  name = QString::fromStdString(site) + ":" +
         QString::fromStdString(program) + "/" +
         QString::fromStdString(function);

  visible = Bool_val(Field(v_, 1));

  v_ = Field(v_, 2);  // field array
  for (unsigned i = 0; i < Wosize_val(v_); i++) {
    fields.emplace_back(Field(v_, i));
  }

  CAMLreturn0;
}

// For sorting sources
bool operator<(
  DashWidgetChart::Source const &a,
  DashWidgetChart::Source const &b)
{
  return a.name < b.name;
}

QDebug operator<<(QDebug debug, DashWidgetChart::Source const &v)
{
  QDebugStateSaver saver(debug);
  debug.nospace() << v.name;

  return debug;
}

DashWidgetChart::DashWidgetChart(value v_) : DashWidget()
{
  CAMLparam1(v_);
  CAMLlocal2(a_, b_);
  assert(Wosize_val(v_) == 4);

  // Title
  title = QString::fromStdString(String_val(Field(v_, 0)));

  // Type
  type = static_cast<ChartType>(Int_val(Field(v_, 1)));

  // Axis
  a_ = Field(v_, 2);  // axes array
  assert(Is_block(a_));
  for (unsigned i = 0; i < Wosize_val(a_); i++) {
    b_ = Field(a_, i);  // i-th axis
    assert(Is_block(b_));
    axes.emplace_back(
      Bool_val(Field(b_, 0)),  // left
      Bool_val(Field(b_, 1)),  // forceZero
      static_cast<enum Axis::Scale>(Int_val(Field(b_, 2)))  // scale
    );
  }

  // Sources
  a_ = Field(v_, 3);  // source array
  assert(Is_block(a_));
  for (unsigned i = 0; i < Wosize_val(a_); i++) {
    b_ = Field(a_, i);  // i-th source
    assert(Is_block(b_));
    sources.emplace_back(b_);
  }

  /* Order sources by name, to make sync easier, minimize updates and
   * aesthetic.
   * Note: although source names are supposed to be all different, better
   * safe than sorry. */
  std::stable_sort(sources.begin(), sources.end());

  CAMLreturn0;
}

DashWidgetChart::DashWidgetChart(
    std::string const sn, std::string const pn, std::string const fn)
  : DashWidget(),
    title(QString::fromStdString(pn) + "/" + QString::fromStdString(fn)),
    type(Plot)
{
  axes.emplace_back(true, false, DashWidgetChart::Axis::Linear);
  sources.emplace_back(sn, pn, fn);
}

value DashWidgetChart::Axis::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();
  ret = caml_alloc(3, 0);
  Store_field(ret, 0, Val_bool(left));
  Store_field(ret, 1, Val_bool(forceZero));
  Store_field(ret, 2, Val_int(static_cast<int>(scale)));
  CAMLreturn(ret);
}

bool DashWidgetChart::Axis::operator==(Axis const &o) const
{
  return left == o.left && forceZero == o.forceZero && scale == o.scale;
}

bool DashWidgetChart::Axis::operator!=(Axis const &o) const
{
  return !(this->operator==(o));
}

DashWidgetChart::Column::Column(value v_)
{
  CAMLparam1(v_);

  assert(6 == Wosize_val(v_));

  double const opacity(Double_val(Field(v_, 0)));
  int const rgb(Int_val(Field(v_, 1)));
  color = QColor(255 & (rgb >> 16), 255 & (rgb >> 8), 255 & rgb, opacity * 255);
  representation = static_cast<Column::Representation>(Int_val(Field(v_, 2)));
  assert(representation >= Unused && representation <= StackCentered);
  name = String_val(Field(v_, 3));
  axisNum = Int_val(Field(v_, 5));
  // Add the factors:
  v_ = Field(v_, 4);  // factors
  for (unsigned j = 0; j < Wosize_val(v_); j++) {
    factors.push_back(String_val(Field(v_, j)));
  }

  CAMLreturn0;
}

DashWidgetChart::Column::Column(
  std::string const &program, std::string const &function,
  std::string const &field)
  : name(field),
    representation(DashWidgetChart::Column::Unused),
    axisNum(0)
{
  QString const fqName(
    QString::fromStdString(program) + "/" +
    QString::fromStdString(function) + "/" +
    QString::fromStdString(field));

  color = colorOfString(fqName);
}

value DashWidgetChart::Column::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, a_);
  checkInOCamlThread();
  ret = caml_alloc(6, 0);
  double const opacity(color.alphaF());
  Store_field(ret, 0, caml_copy_double(opacity));
  int r, g, b;
  color.getRgb(&r, &g, &b);
  int const col(r << 16 | g << 8 | b);
  Store_field(ret, 1, Val_int(col));
  Store_field(ret, 2, Val_int(static_cast<int>(representation)));
  Store_field(ret, 3, caml_copy_string(name.c_str()));
  a_ = caml_alloc(factors.size(), 0);
  for (unsigned i = 0; i < factors.size(); i++) {
    Store_field(a_, i, caml_copy_string(factors[i].c_str()));
  }
  Store_field(ret, 4, a_);
  Store_field(ret, 5, Val_int(axisNum));
  CAMLreturn(ret);
}

QString const DashWidgetChart::Column::nameOfRepresentation(
  DashWidgetChart::Column::Representation rep)
{
  switch (rep) {
    case Unused:
      return QString(QCoreApplication::translate("QMainWindow", "unused"));
    case Independent:
      return QString(QCoreApplication::translate("QMainWindow", "indep."));
    case Stacked:
      return QString(QCoreApplication::translate("QMainWindow", "stack"));
    case StackCentered:
      return QString(QCoreApplication::translate("QMainWindow", "stack+center"));
  }
  assert(false);
}

bool DashWidgetChart::Column::operator==(Column const &o) const
{
  return
    name == o.name && representation == o.representation &&
    axisNum == o.axisNum && factors == o.factors &&
    color == o.color;
}

bool DashWidgetChart::Column::operator!=(Column const &o) const
{
  return !(this->operator==(o));
}

value DashWidgetChart::Source::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal2(ret, a_);
  checkInOCamlThread();

  ret = caml_alloc(3, 0);
  // site_fq
  Store_field(ret, 0, alloc_site_fq(site, program, function));
  // visible
  Store_field(ret, 1, Val_bool(visible));
  // fields
  a_ = caml_alloc(fields.size(), 0);
  size_t i { 0 };
  for (Column const &c : fields) {
    Store_field(a_, i++, c.toOCamlValue());
  }
  assert(i == Wosize_val(a_));
  Store_field(ret, 2, a_);

  CAMLreturn(ret);
}

bool DashWidgetChart::Source::operator==(Source const &o) const
{
  return
    site == o.site && program == o.program && function == o.function &&
    visible == o.visible && fields == o.fields;
}

bool DashWidgetChart::Source::operator!=(Source const &o) const
{
  return !(this->operator==(o));
}

value DashWidgetChart::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal3(ret, widget, a_);
  checkInOCamlThread();
  widget = caml_alloc(4, 1 /* Chart constructor */);
  ret = caml_alloc(1, DashWidgetType);

  Store_field(widget, 0, caml_copy_string(title.toStdString().c_str()));

  Store_field(widget, 1, Val_int(static_cast<int>(type)));

  a_ = caml_alloc(axes.size(), 0);
  for (unsigned i = 0; i < axes.size(); i++) {
    Store_field(a_, i, axes[i].toOCamlValue());
  }
  Store_field(widget, 2, a_);

  a_ = caml_alloc(sources.size(), 0);
  for (unsigned i = 0; i < sources.size(); i++) {
    Store_field(a_, i, sources[i].toOCamlValue());
  }
  Store_field(widget, 3, a_);

  Store_field(ret, 0, widget);
  CAMLreturn(ret);
}

AtomicWidget *DashWidgetChart::editorWidget(
  std::string const &key, QWidget *parent) const
{
  TimeChartEditWidget *editor = new TimeChartEditWidget(nullptr, nullptr, parent);
  editor->setKey(key);
  return editor;
}

bool DashWidgetChart::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  /* The above just guarantee that we have a DashWidget, but not
   * that it is a DashWidgetChart, thus the dynamic_cast: */
  try {
    DashWidgetChart const &o =
      dynamic_cast<DashWidgetChart const &>(other);
    return title == o.title && axes == o.axes && sources == o.sources;
  } catch (std::bad_cast const &) {
    return false;
  }
}

QString const AlertingContact::toQString(std::string const &) const
{
  if (timeout <= 0) return QString();
  return QString(", timeout after ") + stringOfDuration(timeout);
}

AlertingContactIgnore::AlertingContactIgnore(double timeout)
  : AlertingContact(timeout)
{
}

QString const AlertingContactIgnore::toQString(std::string const &key) const
{
  return QString("Ignore") + AlertingContact::toQString(key);
}

bool AlertingContactIgnore::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  try {
    AlertingContactIgnore const &o =
      dynamic_cast<AlertingContactIgnore const &>(other);
    return cmd == o.cmd;
  } catch (std::bad_cast const &) {
    return false;
  }
}

AlertingContactExec::AlertingContactExec(double timeout, value v_)
  : AlertingContact(timeout)
{
  CAMLparam1(v_);

  assert(0 == Tag_val(v_));
  assert(1 == Wosize_val(v_));

  cmd = String_val(Field(v_, 0));

  CAMLreturn0;
}

AlertingContactExec::AlertingContactExec(double timeout, QString const &cmd_)
  : AlertingContact(timeout), cmd(cmd_)
{
}

QString const AlertingContactExec::toQString(std::string const &key) const
{
  return QString("Exec ") + cmd + AlertingContact::toQString(key);
}

bool AlertingContactExec::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  try {
    AlertingContactExec const &o =
      dynamic_cast<AlertingContactExec const &>(other);
    return cmd == o.cmd;
  } catch (std::bad_cast const &) {
    return false;
  }
}

AlertingContactSysLog::AlertingContactSysLog(double timeout, value v_)
  : AlertingContact(timeout)
{
  CAMLparam1(v_);

  assert(1 == Tag_val(v_));
  assert(1 == Wosize_val(v_));

  msg = String_val(Field(v_, 0));

  CAMLreturn0;
}

AlertingContactSysLog::AlertingContactSysLog(double timeout, QString const &msg_)
  : AlertingContact(timeout), msg(msg_)
{
}

QString const AlertingContactSysLog::toQString(std::string const &key) const
{
  return QString("SysLog ") + msg + AlertingContact::toQString(key);
}

bool AlertingContactSysLog::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  try {
    AlertingContactSysLog const &o =
      dynamic_cast<AlertingContactSysLog const &>(other);
    return msg == o.msg;
  } catch (std::bad_cast const &) {
    return false;
  }
}

AlertingContactSqlite::AlertingContactSqlite(double timeout, value v_)
  : AlertingContact(timeout)
{
  CAMLparam1(v_);

  assert(2 == Tag_val(v_));
  assert(3 == Wosize_val(v_));

  file = String_val(Field(v_, 0));
  insert = String_val(Field(v_, 1));
  create = String_val(Field(v_, 2));

  CAMLreturn0;
}

AlertingContactSqlite::AlertingContactSqlite(
  double timeout,
  QString const &file_, QString const &insert_, QString const &create_)
  : AlertingContact(timeout),
    file(file_), insert(insert_), create(create_)
{
}

QString const AlertingContactSqlite::toQString(std::string const &key) const
{
  return QString("SQlite file:") + file + AlertingContact::toQString(key);
}

bool AlertingContactSqlite::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  try {
    AlertingContactSqlite const &o =
      dynamic_cast<AlertingContactSqlite const &>(other);
    return file == o.file && insert == o.insert && create == o.create;
  } catch (std::bad_cast const &) {
    return false;
  }
}

AlertingContactKafka::AlertingContactKafka(double timeout, value v_)
  : AlertingContact(timeout)
{
  CAMLparam1(v_);
  CAMLlocal2(cons_, p_);

  assert(3 == Tag_val(v_));
  assert(4 == Wosize_val(v_));

  for (cons_ = Field(v_, 0); Is_block(cons_); cons_ = Field(cons_, 1)) {
    p_ = Field(cons_, 0);
    options.insert(QPair(
        String_val(Field(p_, 0)),
        String_val(Field(p_, 1))));
  }

  topic = String_val(Field(v_, 1));
  partition = Int_val(Field(v_, 2));
  text = String_val(Field(v_, 3));

  CAMLreturn0;
}

AlertingContactKafka::AlertingContactKafka(
  double timeout,
  QSet<QPair<QString, QString>> const &options_,
  QString const &topic_, unsigned partition_, QString const &text_)
  : AlertingContact(timeout),
    options(options_), topic(topic_), partition(partition_), text(text_)
{
}

QString const AlertingContactKafka::toQString(std::string const &key) const
{
  return QString("Kafka topic:") + topic + AlertingContact::toQString(key);
}

bool AlertingContactKafka::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  try {
    AlertingContactKafka const &o =
      dynamic_cast<AlertingContactKafka const &>(other);
    return topic == o.topic && partition == o.partition && text == o.text &&
           options == o.options;
  } catch (std::bad_cast const &) {
    return false;
  }
}

Notification::Notification(value v_)
  : Value(NotificationType)
{
  CAMLparam1(v_);
  CAMLlocal2(cons_, p_);

  assert(Wosize_val(v_) == 11);

  site = String_val(Field(v_, 0));
  worker = String_val(Field(v_, 1));
  test = Bool_val(Field(v_, 2));
  sentTime = Double_val(Field(v_, 3));
  eventTime =
    Is_block(Field(v_, 4)) ?  // Some...
      std::optional<double>(Double_val(Field(Field(v_, 4), 0))) :
      std::nullopt;
  name = String_val(Field(v_, 5));
  firing = Bool_val(Field(v_, 6));
  certainty = Double_val(Field(v_, 7));
  debounce = Double_val(Field(v_, 8));
  timeout = Double_val(Field(v_, 9));

  for (cons_ = Field(v_, 10); Is_block(cons_); cons_ = Field(cons_, 1)) {
    p_ = Field(cons_, 0);
    parameters.insert(QPair(
        String_val(Field(p_, 0)),
        String_val(Field(p_, 1))));
  }

  CAMLreturn0;
}

QString const Notification::toQString(std::string const &) const
{
  return QString("site:") + site +
         QString(", worker:") + worker +
         QString(", test:") + stringOfBool(test) +
         QString(", name:") + name +
         QString(", firing:") + stringOfBool(firing);
}

bool Notification::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Notification const &o = static_cast<Notification const &>(other);
  return site == o.site && worker == o.worker && test == o.test &&
         sentTime == o.sentTime && eventTime == o.eventTime &&
         name == o.name && firing == o.firing && certainty == o.certainty &&
         debounce == o.debounce && timeout == o.timeout &&
         parameters == o.parameters;
}

DeliveryStatus::DeliveryStatus(value v_)
  : Value(DeliveryStatusType)
{
  assert(! Is_block(v_));

  int s {Int_val(v_)};
  assert(s >= (int)StartToBeSent && s <= (int)StopSent);
  status = static_cast<DeliveryStatus::Status>(s);
}

DeliveryStatus::DeliveryStatus(enum Status status_)
  : Value(DeliveryStatusType),
    status(status_)
{
}

QString const DeliveryStatus::toQString(std::string const &) const
{
  switch (status) {
    case StartToBeSent:
      return QString("StartToBeSent");
    case StartToBeSentThenStopped:
      return QString("StartToBeSentThenStopped");
    case StartSent:
      return QString("StartSent");
    case StartAcked:
      return QString("StartAcked");
    case StopToBeSent:
      return QString("StopToBeSent");
    case StopSent:
      return QString("StopSent");
    case NUM_STATUS:
      break;
  }
  assert(!"DeliveryStatus: invalid status");
}

bool DeliveryStatus::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  DeliveryStatus const &o = static_cast<DeliveryStatus const &>(other);
  return status == o.status;
}

static QString stringOfNotificationOutcome(value v_)
{
  CAMLparam1(v_);

  assert(!Is_block(v_));
  QString ret;
  switch (Int_val(v_)) {
    case 0:
      ret = "duplicate";
      break;
    case 1:
      ret = "inhibited";
      break;
    case 2:
      ret = "silenced";
      break;
    case 3:
      ret = "started escalation";
      break;
    default:
      ret = QString("INVALID notification outcome: %1").arg(Int_val(v_));
      qCritical() << ret;
      break;
  }

  CAMLreturnT(QString, ret);
}

static QString stringOfStopSource(value v_)
{
  CAMLparam1(v_);

  QString ret;
  if (Is_block(v_)) {
    switch (Tag_val(v_)) {
      case 0:
        assert(1 == Wosize_val(v_));
        ret = QString("manually stopped by ") + String_val(Field(v_, 0));
        break;
      case 1:
        assert(1 == Wosize_val(v_));
        ret = QString("timeout for ") + String_val(Field(v_, 0));
        break;
      default:
        ret = QString("INVALID stop source: tag=") + Tag_val(v_);
        break;
    }
  } else {
    switch (Int_val(v_)) {
      case 0:
        ret = "notification";
        break;
      default:
        ret = QString("INVALID stop source: ") + Tag_val(v_);
        break;
    }
  }

  CAMLreturnT(QString, ret);
}

IncidentLog::IncidentLog(value v_)
  : Value(IncidentLogType)
{
  CAMLparam1(v_);
  /* All variants are blocks: */
  assert(Is_block(v_));
  switch (static_cast<IncidentLog::LogTag>(Tag_val(v_))) {
    case IncidentLog::TagNewNotification:
      text = "New notification received, " +
             stringOfNotificationOutcome(Field(v_, 0));
      switch (static_cast<IncidentLog::Outcome>(Int_val(Field(v_, 0)))) {
        case IncidentLog::Duplicate:
          tickKind = TickDup;
          break;
        case IncidentLog::Inhibited:
          // pass
        case IncidentLog::STFU:
          tickKind = TickInhibited;
          break;
        case IncidentLog::Escalate:
          tickKind = TickStart;
          break;
        default:
          assert(!"Invalid Outcome tag");
      }
      break;
    case IncidentLog::TagOutcry:
      text = QString("Sent message via ") + String_val(Field(v_, 0));
      tickKind = IncidentLog::TickOutcry;
      break;
    case IncidentLog::TagAck:
      text = QString("Received ack for ") + String_val(Field(v_, 0));
      tickKind = IncidentLog::TickAck;
      break;
    case IncidentLog::TagStop:
      text = QString("Stopped incident (") +
             stringOfStopSource(Field(v_, 0)) +
             QString(')');
      tickKind = IncidentLog::TickStop;
      break;
    case IncidentLog::TagCancel:
      text = QString("Cancelled message for ") + String_val(Field(v_, 0));
      tickKind = IncidentLog::TickCancel;
      break;
    default:
      assert(!"Invalid IncidentLog tag");
  }
  CAMLreturn0;
}

QString const IncidentLog::toQString(std::string const &key) const
{
  double time;
  if (! parseLogKey(key, nullptr, &time)) {
    qCritical() << "Cannot parse IncidentLog key "
                << QString::fromStdString(key);
    return text;
  }

  return stringOfDate(time) + ": " + text;
}

bool IncidentLog::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  IncidentLog const &o = static_cast<IncidentLog const &>(other);
  (void)o;
  return true;
}

void IncidentLog::paintTick(
  QPainter *painter, qreal width, qreal x, qreal y0, qreal y1) const
{
  qreal const tickWidth { 14 };
  qreal const x0 { x - 0.5 * tickWidth };
  qreal const x1 { x + 0.5 * tickWidth };
  qreal const y { 0.5 * (y0 + y1) };
  qreal const h { y1 - y0 };
  if (x0 > width || x1 < 0) return;
  QPointF const rightTriangle[3] {
    QPointF(x, y0),
    QPointF(x1, y),
    QPointF(x, y1)
  };
  // Same color than BinaryHeatLine blocks
  QColor tickColor { 25, 25, 25 };
  painter->setPen(Qt::NoPen);
  painter->setPen(Qt::NoBrush);
  QPen thick { tickColor };
  thick.setWidth(2);
  painter->setRenderHint(QPainter::Antialiasing, true);

  switch (tickKind) {
    case TickStart:
      painter->setPen(thick);
      painter->drawConvexPolygon(rightTriangle, SIZEOF_ARRAY(rightTriangle));
      break;
    case TickInhibited:
      painter->setPen(thick);
      painter->setBrush(Qt::NoBrush);
      painter->drawConvexPolygon(rightTriangle, SIZEOF_ARRAY(rightTriangle));
      break;
    case TickDup:
      painter->setPen(thick);
      painter->drawLine(x0, y0, x0, y1);
      painter->setPen(Qt::NoPen);
      painter->setBrush(tickColor);
      painter->drawConvexPolygon(rightTriangle, SIZEOF_ARRAY(rightTriangle));
      break;
    case TickOutcry:
      painter->setPen(thick);
      painter->drawLine(x, y0, x, y1);
      painter->setPen(tickColor);
      {
        qreal dr { 0 };
        for (int count = 0; count < 3 && dr > h; count ++, dr += 4) {
          QRectF const sq { x0 + dr, y0 + dr, h - 2*dr, h - 2*dr };
          painter->drawArc(sq, -45*16, 45*16);
        }
      }
      break;
    case TickAck:
      painter->setPen(thick);
      painter->drawLine(x, y0, x, y1);
      painter->setPen(tickColor);
      {
        qreal dr { 0 };
        painter->setPen(tickColor);
        for (int count = 0; count < 3 && dr > h; count ++, dr += 4) {
          QRectF const sq { x0 + dr, y0 + dr, h - 2*dr, h - 2*dr };
          painter->drawArc(sq, 135*16, 225*16);
        }
      }
      break;
    case TickStop:
      {
        QPointF const leftTriangle[3] {
          QPointF(x, y0),
          QPointF(x, y1),
          QPointF(x0, y)
        };
        painter->setBrush(tickColor);
        painter->drawConvexPolygon(leftTriangle, SIZEOF_ARRAY(leftTriangle));
      }
      break;
    case TickCancel:
      painter->setPen(thick);
      painter->drawLine(x0, y0, x1, y1);
      painter->drawLine(x1, y0, x0, y1);
      break;
  }
}

Inhibition::Inhibition(value v_)
  : Value(InhibitionType)
{
  // TODO
  (void)v_;
}

bool Inhibition::operator==(Value const &other) const
{
  if (! Value::operator==(other)) return false;
  Inhibition const &o = static_cast<Inhibition const &>(other);
  (void)o;
  return true;
}

};
