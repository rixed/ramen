#include <cassert>
#include <QtGlobal>
#include <QCheckBox>
#include <QComboBox>
#include <QDebug>
#include <QLineEdit>
#include <QRadioButton>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
# undef alloc
# undef flush
}
#include "AlertInfoEditor.h"
#include "FilterEditor.h"
#include "RamenValue.h" // for checkInOCamlThread
#include "AlertInfo.h"

static bool const verbose(false);

/*
 * SimpleFilter
 */

// Does not alloc on OCaml heap
SimpleFilter::SimpleFilter(value v_)
{
  assert(Is_block(v_));
  assert(Wosize_val(v_) == 3);

  lhs = String_val(Field(v_, 0));
  rhs = String_val(Field(v_, 1));
  op = String_val(Field(v_, 2));
  if (lhs.empty() || rhs.empty() || op.empty())
    qWarning() << "SimpleFilter: received invalid value '"
               << QString::fromStdString(lhs)
               << QString::fromStdString(op)
               << QString::fromStdString(rhs) << "'";
}

SimpleFilter::SimpleFilter(FilterEditor const *e)
{
  assert(e->hasValidInput());
  lhs = e->lhsEdit->text().toStdString();
  rhs = e->rhsEdit->text().toStdString();
  op = e->opEdit->currentData().toString().toStdString();
  assert(!lhs.empty() && !rhs.empty() && !op.empty());
}

value SimpleFilter::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();

  ret = caml_alloc_tuple(3);
  Store_field(ret, 0, caml_copy_string(lhs.c_str()));
  Store_field(ret, 1, caml_copy_string(rhs.c_str()));
  Store_field(ret, 2, caml_copy_string(op.c_str()));

  CAMLreturn(ret);
}

/*
 * ThresholdDistance
 */

ThresholdDistance::ThresholdDistance(value v_)
{
  assert(Is_block(v_));
  assert(Wosize_val(v_) == 1);

  switch (Tag_val(v_)) {
    case 0:  // Absolute
      relative = false;
      break;
    case 1:
      relative = true;
      break;
    default:
      assert(!"Invalid tag for ThresholdDistance");
  }

  v = Double_val(Field(v_, 0));
}

value ThresholdDistance::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();

  ret = caml_alloc(1, relative ? 1 : 0);
  Store_field(ret, 0, caml_copy_double(v));

  CAMLreturn(ret);
}

/*
 * Threshold
 */

ConstantThreshold::ConstantThreshold(double v_) : v(v_)
{
}

value ConstantThreshold::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();

  ret = caml_alloc(1, 0 /* Constant */);
  Store_field(ret, 0, caml_copy_double(v));

  CAMLreturn(ret);
}

bool ConstantThreshold::operator==(Threshold const &other) const
{
  try {
    ConstantThreshold const &o =
      dynamic_cast<ConstantThreshold const &>(other);
    return isClose(v, o.v);
  } catch (std::bad_cast const &) {
    return false;
  }
}

Baseline::Baseline(
  double avgWindow_, int sampleSize_, double percentile_,
  int seasonality_, double smoothFactor_,
  ThresholdDistance const &maxDistance_)
  : avgWindow(avgWindow_), sampleSize(sampleSize_), percentile(percentile_),
    seasonality(seasonality_), smoothFactor(smoothFactor_),
    maxDistance(maxDistance_)
{
}

value Baseline::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal1(ret);
  checkInOCamlThread();

  ret = caml_alloc(6, 1 /* Baseline */);
  Store_field(ret, 0, caml_copy_double(avgWindow));
  Store_field(ret, 1, Val_int(sampleSize));
  Store_field(ret, 2, caml_copy_double(percentile));
  Store_field(ret, 3, Val_int(seasonality));
  Store_field(ret, 4, caml_copy_double(smoothFactor));
  Store_field(ret, 5, maxDistance.toOCamlValue());

  CAMLreturn(ret);
}

bool Baseline::operator==(Threshold const &other) const
{
  try {
    Baseline const &o =
      dynamic_cast<Baseline const &>(other);
    return sampleSize == o.sampleSize &&
           seasonality == o.seasonality &&
           isClose(avgWindow, o.avgWindow) &&
           isClose(percentile, o.percentile) &&
           isClose(smoothFactor, o.smoothFactor);
  } catch (std::bad_cast const &) {
    return false;
  }
}

std::unique_ptr<Threshold const> Threshold::ofOCaml(value v_)
{
  assert(Is_block(v_));

  switch (Tag_val(v_)) {
    case 0:  // Constant
      assert(Wosize_val(v_) == 1);
      return std::make_unique<ConstantThreshold const>(Double_val(Field(v_, 0)));
    case 1:  // Baseline
      {
        assert(Wosize_val(v_) == 6);
        ThresholdDistance *maxDistance { new ThresholdDistance(Field(v_, 5)) };
        return std::make_unique<Baseline const>(
          Double_val(Field(v_, 0)),
          Int_val(Field(v_, 1)),
          Double_val(Field(v_, 2)),
          Int_val(Field(v_, 3)),
          Double_val(Field(v_, 4)),
          *maxDistance);
      }
    default:
      assert(!"Invalid tag for Threshold");
      return nullptr;
  }
}

/*
 * AlertInfo
 */

// Does not alloc on OCaml heap
AlertInfo::AlertInfo(value v_)
{
  assert(Is_block(v_));
  assert(17 == Wosize_val(v_));

  table = String_val(Field(v_, 0));
  column = String_val(Field(v_, 1));
  isEnabled = Bool_val(Field(v_, 2));
  for (value cons = Field(v_, 3); Is_block(cons); cons = Field(cons, 1))
    where.emplace_back<SimpleFilter>(Field(cons, 0));
  if (Is_block(Field(v_, 4))) {
    assert(Tag_val(Field(v_, 4)) == 0);  // Some
    groupBy = std::make_optional<std::set<std::string>>();
    for (value cons = Field(Field(v_, 4), 0); Is_block(cons); cons = Field(cons, 1))
      groupBy->emplace<std::string>(String_val(Field(cons, 0)));
  } else {  // None
    groupBy.reset();
  }
  for (value cons = Field(v_, 5); Is_block(cons); cons = Field(cons, 1))
    having.emplace_back<SimpleFilter>(Field(cons, 0));
  threshold = Threshold::ofOCaml(Field(v_, 6));
  hysteresis = Double_val(Field(v_, 7));
  duration = Double_val(Field(v_, 8));
  ratio = Double_val(Field(v_, 9));
  timeStep = Double_val(Field(v_, 10));
  for (value cons = Field(v_, 11); Is_block(cons); cons = Field(cons, 1))
    tops.push_back(String_val(Field(cons, 0)));
  for (value cons = Field(v_, 12); Is_block(cons); cons = Field(cons, 1))
    carryFields.push_back(String_val(Field(cons, 0)));
  for (value cons = Field(v_, 13); Is_block(cons); cons = Field(cons, 1))
    carryCsts.emplace_back(
      String_val(Field(Field(cons, 0), 0)),
      String_val(Field(Field(cons, 0), 1)));
  id = String_val(Field(v_, 14));
  descTitle = String_val(Field(v_, 15));
  descFiring = String_val(Field(v_, 16));
  descRecovery = String_val(Field(v_, 17));
}

AlertInfo::AlertInfo(AlertInfoEditor const *editor)
{
  /* try to get table and column from the source and fallback to
   * saved ones if no entry is selected: */
  table = editor->getTable();
  column = editor->getColumn();
  isEnabled = editor->isEnabled->isChecked();
  // TODO: baseline type of threshold
  double cst_threshold = editor->threshold->text().toDouble();
  threshold = std::make_unique<ConstantThreshold const>(cst_threshold);
  hysteresis = editor->hysteresis->text().toDouble();
  // TODO: support multiple where/having
  if (!editor->where->isEmpty() && editor->where->hasValidInput())
    where.emplace_back<SimpleFilter>(editor->where);
  groupBy = editor->getGroupBy();
  if (!editor->having->isEmpty() && editor->having->hasValidInput())
    having.emplace_back<SimpleFilter>(editor->having);
  duration = editor->duration->text().toDouble();
  ratio = 0.01 * editor->percentage->text().toDouble();
  timeStep = editor->timeStep->text().toDouble();

  // TODO: support multiple tops/carry
  if (!editor->top->text().isEmpty())
    tops.emplace_back<std::string>(editor->top->text().toStdString());
  if (!editor->carryFields->text().isEmpty())
    carryFields.emplace_back<std::string>(editor->carryFields->text().toStdString());
  // TODO: carryCsts

  id = editor->id->text().toStdString();
  descTitle = editor->descTitle->text().toStdString();
  descFiring = editor->descFiring->text().toStdString();
  descRecovery = editor->descRecovery->text().toStdString();
}

// This _does_ alloc on OCaml's heap
value AlertInfo::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal4(ret, some_lst, cons, pair);
  checkInOCamlThread();

  ret = caml_alloc_tuple(17);
  Store_field(ret, 0, caml_copy_string(table.c_str()));
  Store_field(ret, 1, caml_copy_string(column.c_str()));
  Store_field(ret, 2, Val_bool(isEnabled));
  Store_field(ret, 3, Val_emptylist);
  for (auto const &f : where) {
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 0, f.toOCamlValue());
    Store_field(cons, 1, Field(ret, 3));
    Store_field(ret, 3, cons);
  }
  if (groupBy) {
    some_lst = caml_alloc(1, 0 /* Some */);
    Store_field(ret, 4, some_lst);
    for (auto const &k : *groupBy) {
      cons = caml_alloc(2, Tag_cons);
      Store_field(cons, 0, caml_copy_string(k.c_str()));
      Store_field(cons, 1, Field(some_lst, 0));
      Store_field(some_lst, 0, cons);
    }
  } else {
    Store_field(ret, 4, Val_int(0)); // None
  }
  Store_field(ret, 5, Val_emptylist);
  for (auto const &f : having) {
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 0, f.toOCamlValue());
    Store_field(cons, 1, Field(ret, 5));
    Store_field(ret, 5, cons);
  }
  Store_field(ret, 6, threshold->toOCamlValue());
  Store_field(ret, 7, caml_copy_double(hysteresis));
  Store_field(ret, 8, caml_copy_double(duration));
  Store_field(ret, 9, caml_copy_double(ratio));
  Store_field(ret, 10, caml_copy_double(timeStep));
  Store_field(ret, 11, Val_emptylist);
  for (auto const &f : tops) {
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 0, caml_copy_string(f.c_str()));
    Store_field(cons, 1, Field(ret, 11));
    Store_field(ret, 11, cons);
  }
  Store_field(ret, 12, Val_emptylist);
  for (auto const &f : carryFields) {
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 0, caml_copy_string(f.c_str()));
    Store_field(cons, 1, Field(ret, 12));
    Store_field(ret, 12, cons);
  }
  Store_field(ret, 13, Val_emptylist);
  for (auto const &f : carryCsts) {
    pair = caml_alloc_tuple(2);
    Store_field(pair, 0, caml_copy_string(f.first.c_str()));
    Store_field(pair, 1, caml_copy_string(f.second.c_str()));
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 0, pair);
    Store_field(cons, 1, Field(ret, 13));
    Store_field(ret, 13, cons);
  }
  Store_field(ret, 14, caml_copy_string(id.c_str()));
  Store_field(ret, 15, caml_copy_string(descTitle.c_str()));
  Store_field(ret, 16, caml_copy_string(descFiring.c_str()));
  Store_field(ret, 17, caml_copy_string(descRecovery.c_str()));

  CAMLreturn(ret);
}

QString const AlertInfo::toQString() const
{
  return
    QString("Alert on ") +
    QString::fromStdString(table) + "/" +
    QString::fromStdString(column);
}

bool AlertInfo::operator==(AlertInfo const &that) const
{
  if (! (table == that.table)) {
    if (verbose) qDebug() << "AlertInfo: table differs";
    return false;
  }
  if (! (column == that.column)) {
    if (verbose) qDebug() << "AlertInfo: column differs";
    return false;
  }
  if (! (isEnabled == that.isEnabled)) {
    if (verbose) qDebug() << "AlertInfo: isEnabled differs";
    return false;
  }
  if (! (*threshold == *that.threshold)) {
    if (verbose) qDebug() << "AlertInfo: threshold differs";
    return false;
  }
  if (! isClose(hysteresis, that.hysteresis)) {
    if (verbose) qDebug() << "AlertInfo: recovery differs";
    return false;
  }
  if (! isClose(duration, that.duration)) {
    if (verbose) qDebug() << "AlertInfo: duration differs";
    return false;
  }
  if (! isClose(ratio, that.ratio)) {
    if (verbose) qDebug() << "AlertInfo: ratio differs";
    return false;
  }
  if (! isClose(timeStep, that.timeStep)) {
    if (verbose) qDebug() << "AlertInfo: timeStep differs";
    return false;
  }
  if (! (id == that.id)) {
    if (verbose) qDebug() << "AlertInfo: id differs";
    return false;
  }
  if (! (descTitle == that.descTitle)) {
    if (verbose) qDebug() << "AlertInfo: descTitle differs";
    return false;
  }
  if (! (descFiring == that.descFiring)) {
    if (verbose) qDebug() << "AlertInfo: descFiring differs";
    return false;
  }
  if (! (descRecovery == that.descRecovery)) {
    if (verbose) qDebug() << "AlertInfo: descRecovery differs";
    return false;
  }

  if (
    (!groupBy && !that.groupBy) ||
    (*groupBy != *that.groupBy)
  ) {
    if (verbose) qDebug() << "AlertInfo: groupBy fields differ";
    return false;
  }

  if (! (tops == that.tops)) {
    if (verbose) qDebug() << "AlertInfo: top fields differ";
    return false;
  }

  if (! (carryFields == that.carryFields)) {
    if (verbose) qDebug() << "AlertInfo: top fields differ";
    return false;
  }

  if (! (carryCsts == that.carryCsts)) {
    if (verbose) qDebug() << "AlertInfo: top fields differ";
    return false;
  }

  if (! (where == that.where)) {
    if (verbose) qDebug() << "AlertInfo: where filter differs";
    return false;
  }

  if (! (having == that.having)) {
    if (verbose) qDebug() << "AlertInfo: having filter differs";
    return false;
  }

  return true;
}
