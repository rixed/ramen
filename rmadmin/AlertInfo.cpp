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

// Does not alloc on OCaml heap
SimpleFilter::SimpleFilter(value v_)
{
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

// Does not alloc on OCaml heap
AlertInfo::AlertInfo(value v_)
{
  assert(0 == Tag_val(v_));
  assert(1 == Wosize_val(v_));
  v_ = Field(v_, 0);

  assert(Wosize_val(v_) == 16);

  table = String_val(Field(v_, 0));
  column = String_val(Field(v_, 1));
  isEnabled = Bool_val(Field(v_, 2));
  for (value cons = Field(v_, 3); Is_block(cons); cons = Field(cons, 1))
    where.emplace_back<SimpleFilter>(Field(cons, 0));
  for (value cons = Field(v_, 4); Is_block(cons); cons = Field(cons, 1))
    having.emplace_back<SimpleFilter>(Field(cons, 0));
  threshold = Double_val(Field(v_, 5));
  recovery = Double_val(Field(v_, 6));
  duration = Double_val(Field(v_, 7));
  ratio = Double_val(Field(v_, 8));
  timeStep = Double_val(Field(v_, 9));
  for (value cons = Field(v_, 10); Is_block(cons); cons = Field(cons, 1))
    tops.push_back(String_val(Field(cons, 0)));
  for (value cons = Field(v_, 11); Is_block(cons); cons = Field(cons, 1))
    carry.push_back(String_val(Field(cons, 0)));
  id = String_val(Field(v_, 12));
  descTitle = String_val(Field(v_, 13));
  descFiring = String_val(Field(v_, 14));
  descRecovery = String_val(Field(v_, 15));
}

AlertInfo::AlertInfo(AlertInfoEditor const *editor)
{
  /* try to get table and column from the source and fallback to
   * saved ones if no entry is selected: */
  table = editor->getTable();
  column = editor->getColumn();
  isEnabled = editor->isEnabled->isChecked();
  threshold = editor->threshold->text().toDouble();
  double const hysteresis = editor->hysteresis->text().toDouble();
  double const margin = 0.01 * hysteresis * threshold;
  recovery = editor->thresholdIsMax->isChecked() ? threshold - margin :
                                                   threshold + margin;
  // TODO: support multiple where/having
  if (!editor->where->isEmpty() && editor->where->hasValidInput())
    where.emplace_back<SimpleFilter>(editor->where);
  if (!editor->having->isEmpty() && editor->having->hasValidInput())
    having.emplace_back<SimpleFilter>(editor->having);
  duration = editor->duration->text().toDouble();
  ratio = 0.01 * editor->percentage->text().toDouble();
  timeStep = editor->timeStep->text().toDouble();

  // TODO: support multiple tops/carry
  if (!editor->top->text().isEmpty())
    tops.emplace_back<std::string>(editor->top->text().toStdString());
  if (!editor->carry->text().isEmpty())
    carry.emplace_back<std::string>(editor->carry->text().toStdString());

  id = editor->id->text().toStdString();
  descTitle = editor->descTitle->text().toStdString();
  descFiring = editor->descFiring->text().toStdString();
  descRecovery = editor->descRecovery->text().toStdString();
}

// This _does_ alloc on OCaml's heap
value AlertInfo::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal4(ret, v1, lst, cons);
  checkInOCamlThread();

  v1 = caml_alloc_tuple(16);
  Store_field(v1, 0, caml_copy_string(table.c_str()));
  Store_field(v1, 1, caml_copy_string(column.c_str()));
  Store_field(v1, 2, Val_bool(isEnabled));
  Store_field(v1, 3, Val_emptylist);
  for (auto const &f : where) {
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 0, f.toOCamlValue());
    Store_field(cons, 1, Field(v1, 3));
    Store_field(v1, 3, cons);
  }
  Store_field(v1, 4, Val_emptylist);
  for (auto const &f : having) {
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 0, f.toOCamlValue());
    Store_field(cons, 1, Field(v1, 4));
    Store_field(v1, 4, cons);
  }
  Store_field(v1, 5, caml_copy_double(threshold));
  Store_field(v1, 6, caml_copy_double(recovery));
  Store_field(v1, 7, caml_copy_double(duration));
  Store_field(v1, 8, caml_copy_double(ratio));
  Store_field(v1, 9, caml_copy_double(timeStep));
  Store_field(v1, 10, Val_emptylist);
  for (auto const &f : tops) {
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 0, caml_copy_string(f.c_str()));
    Store_field(cons, 1, Field(v1, 10));
    Store_field(v1, 10, cons);
  }
  Store_field(v1, 11, Val_emptylist);
  for (auto const &f : carry) {
    cons = caml_alloc(2, Tag_cons);
    Store_field(cons, 0, caml_copy_string(f.c_str()));
    Store_field(cons, 1, Field(v1, 10));
    Store_field(v1, 11, cons);
  }

  Store_field(v1, 12, caml_copy_string(id.c_str()));
  Store_field(v1, 13, caml_copy_string(descTitle.c_str()));
  Store_field(v1, 14, caml_copy_string(descFiring.c_str()));
  Store_field(v1, 15, caml_copy_string(descRecovery.c_str()));

  ret = caml_alloc(1 /* 1 field */, 0 /* tag = V1 */);
  Store_field(ret, 0, v1);

  CAMLreturn(ret);
}

QString const AlertInfo::toQString() const
{
  return
    QString("Alert v1 on ") +
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
  if (! isClose(threshold, that.threshold)) {
    if (verbose) qDebug() << "AlertInfo: threshold differs";
    return false;
  }
  if (! (recovery == that.recovery)) {
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

  if (! (tops == that.tops)) {
    if (verbose) qDebug() << "AlertInfo: top fields differ";
    return false;
  }

  if (! (carry == that.carry)) {
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
