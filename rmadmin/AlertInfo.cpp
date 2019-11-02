#include <cassert>
#include <cmath>
#include <QtGlobal>
#include <QDebug>
#include <QHBoxLayout>
#include <QVBoxLayout>
#include <QFormLayout>
#include <QLineEdit>
#include <QCheckBox>
#include <QGroupBox>
#include <QRadioButton>
#include <QLabel>
extern "C" {
# include <caml/memory.h>
# include <caml/alloc.h>
# include <caml/custom.h>
# undef alloc
# undef flush
}
#include "RamenValue.h" // for checkInOCamlThread
#include "NamesTree.h"
#include "confValue.h"
#include "AlertInfo.h"

static bool const verbose = true;

NameTreeView::NameTreeView(QWidget *parent) :
  QTreeView(parent)
{
  setUniformRowHeights(true);
  setHeaderHidden(true);
  expandAll();
  setSelectionMode(QAbstractItemView::SingleSelection);
}

void NameTreeView::currentChanged(
  const QModelIndex &current, const QModelIndex &previous)
{
  QAbstractItemView::currentChanged(current, previous);
  emit selectedChanged(current);
}

// Does not alloc on OCaml heap
AlertInfoV1::SimpleFilter::SimpleFilter(value v_)
{
  assert(Wosize_val(v_) == 3);

  lhs = String_val(Field(v_, 0));
  rhs = String_val(Field(v_, 1));
  op = String_val(Field(v_, 2));
}

value AlertInfoV1::SimpleFilter::toOCamlValue() const
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

QWidget *AlertInfoV1::SimpleFilter::editorWidget() const
{
  /* In theory, lhs must be a field. But actually, we can generalise for free
   * to any terminal  expression (immediate value or field name) while still
   * offering good input guidance.
   * Let's consider that both lhs and rhs can be any terminal, and try to
   * guide the input in a line input using a QCompleter in a QLineEdit. */
  return new SimpleFilterEditor(this);
}

// Does not alloc on OCaml heap
AlertInfoV1::AlertInfoV1(value v_)
{
  assert(0 == Tag_val(v_));
  assert(1 == Wosize_val(v_));
  v_ = Field(v_, 0);

  assert(Wosize_val(v_) == 14);

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
  id = String_val(Field(v_, 10));
  descTitle = String_val(Field(v_, 11));
  descFiring = String_val(Field(v_, 12));
  descRecovery = String_val(Field(v_, 13));
}

AlertInfoV1::AlertInfoV1(AlertInfoV1Editor const *editor)
{
  /* try to get table and column from the source and fallback to
   * saved ones if no entry is selected: */
  NamesTree *model = static_cast<NamesTree *>(editor->source->model());
  std::pair<std::string, std::string> path =
    model->pathOfIndex(editor->source->currentIndex());
  table = path.first.empty() ? editor->table : path.first;
  column = path.second.empty() ? editor->column : path.second;
  isEnabled = editor->isEnabled->isChecked();
  // TODO: where, having
  threshold = editor->threshold->text().toDouble();
  double const hysteresis = editor->hysteresis->text().toDouble();
  double const margin = hysteresis * threshold;
  recovery = editor->thresholdIsMax->isChecked() ? threshold - margin :
                                                  threshold + margin;
  duration = editor->duration->text().toDouble();
  ratio = editor->ratio->text().toDouble();
  timeStep = editor->timeStep;
  id = editor->id->text().toStdString();
  descTitle = editor->descTitle->text().toStdString();
  descFiring = editor->descFiring->text().toStdString();
  descRecovery = editor->descRecovery->text().toStdString();
}

// This _does_ alloc on OCaml's heap
value AlertInfoV1::toOCamlValue() const
{
  CAMLparam0();
  CAMLlocal4(ret, v1, lst, cons);
  checkInOCamlThread();

  v1 = caml_alloc_tuple(14);
  Store_field(v1, 0, caml_copy_string(table.c_str()));
  Store_field(v1, 1, caml_copy_string(column.c_str()));
  Store_field(v1, 2, Val_bool(isEnabled));
  Store_field(v1, 3, Val_emptylist);
  for (auto f : where) {
    Store_field(cons, 0, f.toOCamlValue());
    Store_field(cons, 1, Field(v1, 3));
    Store_field(v1, 3, cons);
  }
  Store_field(v1, 4, Val_emptylist);
  for (auto f : having) {
    Store_field(cons, 0, f.toOCamlValue());
    Store_field(cons, 1, Field(v1, 4));
    Store_field(v1, 4, cons);
  }
  Store_field(v1, 5, caml_copy_double(threshold));
  Store_field(v1, 6, caml_copy_double(recovery));
  Store_field(v1, 7, caml_copy_double(duration));
  Store_field(v1, 8, caml_copy_double(ratio));
  Store_field(v1, 9, caml_copy_double(timeStep));
  Store_field(v1, 10, caml_copy_string(id.c_str()));
  Store_field(v1, 11, caml_copy_string(descTitle.c_str()));
  Store_field(v1, 12, caml_copy_string(descFiring.c_str()));
  Store_field(v1, 13, caml_copy_string(descRecovery.c_str()));

  ret = caml_alloc(1 /* 1 field */, 0 /* tag = V1 */);
  Store_field(ret, 0, v1);

  CAMLreturn(ret);
}

QString const AlertInfoV1::toQString() const
{
  return
    QString("Alert v1 on ") +
    QString::fromStdString(table) + "/" +
    QString::fromStdString(column);
}

bool AlertInfoV1::operator==(AlertInfoV1 const &that) const
{
  // TODO: where and having
  if (! (table == that.table)) {
    if (verbose) qDebug() << "AlertInfoV1: table differs";
    return false;
  }
  if (! (column == that.column)) {
    if (verbose) qDebug() << "AlertInfoV1: column differs";
    return false;
  }
  if (! (isEnabled == that.isEnabled)) {
    if (verbose) qDebug() << "AlertInfoV1: isEnabled differs";
    return false;
  }
  if (! isClose(threshold, that.threshold)) {
    if (verbose) qDebug() << "AlertInfoV1: threshold differs";
    return false;
  }
  if (! (recovery == that.recovery)) {
    if (verbose) qDebug() << "AlertInfoV1: recovery differs";
    return false;
  }
  if (! isClose(duration, that.duration)) {
    if (verbose) qDebug() << "AlertInfoV1: duration differs";
    return false;
  }
  if (! isClose(ratio, that.ratio)) {
    if (verbose) qDebug() << "AlertInfoV1: ratio differs";
    return false;
  }
  if (! isClose(timeStep, that.timeStep)) {
    if (verbose) qDebug() << "AlertInfoV1: timeStep differs";
    return false;
  }
  if (! (id == that.id)) {
    if (verbose) qDebug() << "AlertInfoV1: id differs";
    return false;
  }
  if (! (descTitle == that.descTitle)) {
    if (verbose) qDebug() << "AlertInfoV1: descTitle differs";
    return false;
  }
  if (! (descFiring == that.descFiring)) {
    if (verbose) qDebug() << "AlertInfoV1: descFiring differs";
    return false;
  }
  if (! (descRecovery == that.descRecovery)) {
    if (verbose) qDebug() << "AlertInfoV1: descRecovery differs";
    return false;
  }

  return true;
}

bool AlertInfoV1::operator==(AlertInfo const &that_) const
{
  /* For now there is only one kind of AlertInfo: */
  AlertInfoV1 const &that = static_cast<AlertInfoV1 const &>(that_);
  return operator==(that);
}

SimpleFilterEditor::SimpleFilterEditor(
  AlertInfoV1::SimpleFilter const *filter, QWidget *parent) :
  QWidget(parent)
{
  // TODO: Completer for a given fq
  lhsEdit = new QLineEdit(QString::fromStdString(filter->lhs));
  rhsEdit = new QLineEdit(QString::fromStdString(filter->rhs));
  opEdit = new QLineEdit(QString::fromStdString(filter->op));

  QHBoxLayout *layout = new QHBoxLayout;
  layout->addWidget(lhsEdit);
  layout->addWidget(opEdit);
  layout->addWidget(rhsEdit);
  setLayout(layout);
}

void SimpleFilterEditor::setEnabled(bool enabled)
{
  lhsEdit->setEnabled(enabled);
  rhsEdit->setEnabled(enabled);
  opEdit->setEnabled(enabled);
}

AlertInfoV1Editor::AlertInfoV1Editor(QWidget *parent) :
  QWidget(parent)
{
  source = new NameTreeView;
  // TODO: restrict to numerical fields
  // QTreeView to select the parent function field (aka "table" + "column")
  source->setModel(NamesTree::globalNamesTreeAnySites);
  connect(source, &NameTreeView::selectedChanged,
          this, &AlertInfoV1Editor::checkSource);

  /* The text is reset with the proper table/column name when an
   * error is detected: */
  inexistantSourceError = new QLabel;
  inexistantSourceError->hide();
  inexistantSourceError->setStyleSheet("color: red;");

  mustSelectAField = new QLabel(tr("Please select a field name"));

  isEnabled = new QCheckBox(tr("enabled"));
  isEnabled->setChecked(true);

  // TODO: SimpleFiltersEditor, as a list of SimpleFilterEditors.

  // TODO: proper validators
  threshold = new QLineEdit;
  hysteresis = new QLineEdit;
  duration = new QLineEdit;
  ratio = new QLineEdit;
  id = new QLineEdit;

  descTitle = new QLineEdit;
  descFiring = new QLineEdit;
  descRecovery = new QLineEdit;

  QLabel *description = new QLabel;

  /* Layout: Starts with a dynamic sentence describing the notification, and
   * then the form: */

  QVBoxLayout *outerLayout = new QVBoxLayout;
  {
    // The names and descriptions
    QGroupBox *namesBox = new QGroupBox(tr("Names"));
    QFormLayout *namesLayout = new QFormLayout;
    {
      namesLayout->addRow(tr("Alert unique name:"), descTitle);
      namesLayout->addRow(tr("Optional Identifier:"), id);
      namesLayout->addRow(tr("Text when firing:"), descFiring);
      namesLayout->addRow(tr("Text when recovering:"), descRecovery);
    }
    namesBox->setLayout(namesLayout);
    outerLayout->addWidget(namesBox);

    // The notification condition
    QGroupBox *condition = new QGroupBox(tr("Main Condition"));
    QFormLayout *conditionLayout = new QFormLayout;
    {
      conditionLayout->addRow(tr("Metric:"), source);
      conditionLayout->addWidget(inexistantSourceError);
      conditionLayout->addWidget(mustSelectAField);

      // TODO: WHERE

      thresholdIsMax = new QRadioButton(tr("max"));
      thresholdIsMin = new QRadioButton(tr("min"));
      thresholdIsMax->setChecked(true);
      QHBoxLayout *limitLayout = new QHBoxLayout;
      {
        QVBoxLayout *minMaxOuterLayout = new QVBoxLayout;
        {
          QHBoxLayout *minMaxLayout = new QHBoxLayout;
          minMaxLayout->addWidget(thresholdIsMax);
          minMaxLayout->addWidget(thresholdIsMin);
          minMaxOuterLayout->addLayout(minMaxLayout);
          minMaxOuterLayout->addStretch();
        }
        limitLayout->addLayout(minMaxOuterLayout);
      }
      {
        QFormLayout *thresholdLayout = new QFormLayout;
        thresholdLayout->addRow(tr("threshold:"), threshold);
        thresholdLayout->addRow(tr("hysteresis:"), hysteresis);
        limitLayout->addLayout(thresholdLayout);
      }
      QWidget *minMaxBox = new QWidget;
      minMaxBox->setLayout(limitLayout);
      conditionLayout->addWidget(minMaxBox);

      QFormLayout *durationForm = new QFormLayout;
      durationForm->addRow(tr("Ratio of measurements:"), ratio);
      durationForm->addRow(tr("During the last:"), duration);
      QWidget *durationBox = new QWidget;
      durationBox->setLayout(durationForm);
      conditionLayout->addRow(tr("Minimum Duration:"), durationBox);
    }
    condition->setLayout(conditionLayout);
    outerLayout->addWidget(condition);

    // Additional conditions
    QGroupBox *addConditions = new QGroupBox(tr("Additional Conditions"));
    // TODO: HAVING
    outerLayout->addWidget(addConditions);

    // The description
    QGroupBox *descriptionBox = new QGroupBox(tr("Description"));
    QVBoxLayout *descriptionBoxLayout = new QVBoxLayout;
    {
      descriptionBoxLayout->addWidget(description);
    }
    descriptionBox->setLayout(descriptionBoxLayout);
    outerLayout->addWidget(descriptionBox);

    // Final
    outerLayout->addWidget(isEnabled);
  }
  setLayout(outerLayout);
}

void AlertInfoV1Editor::setEnabled(bool enabled)
{
  source->setEnabled(enabled);
  isEnabled->setEnabled(enabled);
  thresholdIsMax->setEnabled(enabled);
  thresholdIsMin->setEnabled(enabled);
  threshold->setEnabled(enabled);
  hysteresis->setEnabled(enabled);
  duration->setEnabled(enabled);
  ratio->setEnabled(enabled);
  id->setEnabled(enabled);
  descTitle->setEnabled(enabled);
  descFiring->setEnabled(enabled);
  descRecovery->setEnabled(enabled);

  if (enabled) {
    checkSource(source->currentIndex());
  } else {
    mustSelectAField->hide();
  }
}

bool AlertInfoV1Editor::setValue(AlertInfoV1 const &v1)
{
  /* Source:
   * Look for the name "$table/$column" and select it, but
   * also save the table and column names in case they are not
   * known (for instance if the program is not running (yet)). */
  table = v1.table;
  column = v1.column;
  NamesTree *model = static_cast<NamesTree *>(source->model());
  std::string const path(v1.table + "/" + v1.column);
  QModelIndex index(model->find(path));
  if (index.isValid()) {
    source->setCurrentIndex(index);
    checkSource(index);
  } else {
    if (verbose)
      qDebug() << "Cannot find field" << QString::fromStdString(path);
    inexistantSourceError->setText(
      tr("Field %1/%2 does not exist")
      .arg(QString::fromStdString(v1.table))
      .arg(QString::fromStdString(v1.column)));
    inexistantSourceError->show();
  }

  isEnabled->setChecked(v1.isEnabled);

  threshold->setText(QString::number(v1.threshold));

  double const h =
    v1.recovery == v1.threshold ?
      0. :
      abs(v1.recovery - v1.threshold) /
      fmax(abs(v1.recovery), abs(v1.threshold));
  hysteresis->setText(QString::number(h));

  duration->setText(QString::number(v1.duration));

  ratio->setText(QString::number(v1.ratio));

  timeStep = v1.timeStep; // just preserve that for now

  id->setText(QString::fromStdString(v1.id));

  descTitle->setText(QString::fromStdString(v1.descTitle));

  descFiring->setText(QString::fromStdString(v1.descFiring));

  descRecovery->setText(QString::fromStdString(v1.descRecovery));

  return true;
}

std::unique_ptr<AlertInfoV1> AlertInfoV1Editor::getValue() const
{
  return std::make_unique<AlertInfoV1>(this);
}

/* This is called each time we change or set the source to some value: */
void AlertInfoV1Editor::checkSource(QModelIndex const &current) const
{
  inexistantSourceError->hide();
  NamesTree *model = static_cast<NamesTree *>(source->model());
  mustSelectAField->setVisible(! model->isField(current));

  emit inputChanged();
}

/* Now the AtomicWidget to edit alerting info (of any version): */

AlertInfoEditor::AlertInfoEditor(QWidget *parent) :
  AtomicWidget(parent)
{
  v1 = new AlertInfoV1Editor;
  relayoutWidget(v1);

  connect(v1, &AlertInfoV1Editor::inputChanged,
          this, &AlertInfoEditor::inputChanged);
}

std::shared_ptr<conf::Value const> AlertInfoEditor::getValue() const
{
  std::unique_ptr<AlertInfo> info(v1->getValue());

  return std::shared_ptr<conf::Value const>(
    new conf::Alert(std::move(info)));
}

void AlertInfoEditor::setEnabled(bool enabled)
{
  v1->setEnabled(enabled);
}

bool AlertInfoEditor::setValue(
  std::string const &, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::Alert const> alert =
    std::dynamic_pointer_cast<conf::Alert const>(v);

  if (! alert) {
    qCritical() << "Not a conf::Alert?!";
    return false;
  }

  AlertInfoV1 *info = static_cast<AlertInfoV1 *>(alert->info);
  return v1->setValue(*info);
}
