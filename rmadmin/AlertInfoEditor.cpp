#include <cmath>
#include <QCheckBox>
#include <QDebug>
#include <QDoubleValidator>
#include <QFormLayout>
#include <QGroupBox>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QRadioButton>
#include <QRegularExpressionValidator>
#include <QVBoxLayout>
#include "NamesTree.h"
#include "confValue.h"
#include "misc.h"
#include "AlertInfoEditor.h"

static bool const verbose = true;

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
/*  connect(source->model(), &NamesTree::rowsInserted,
          source, &NameTreeView::expand);*/

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
  threshold->setValidator(new QDoubleValidator);
  hysteresis = new QLineEdit;
  hysteresis->setValidator(new QDoubleValidator(0., 100., 5));
  duration = new QLineEdit;
  duration->setValidator(new QDoubleValidator(0., std::numeric_limits<double>::max(), 5)); // TODO: DurationValidator
  percentage = new QLineEdit;
  percentage->setValidator(new QDoubleValidator(0., 100., 5));
  timeStep = 30;  // TODO?
  id = new QLineEdit;

  descTitle = new QLineEdit;
  /* At least one char that's not a space. Beware that validators
   * automatically anchor the pattern: */
  QRegularExpression nonEmpty(".*\\S+.*");
  descTitle->setValidator(new QRegularExpressionValidator(nonEmpty));
  descFiring = new QLineEdit;
  descRecovery = new QLineEdit;

  description = new QLabel;

  /* Layout: Starts with a dynamic sentence describing the notification, and
   * then the form: */

  QVBoxLayout *outerLayout = new QVBoxLayout;
  {
    // The names and descriptions
    QGroupBox *namesBox = new QGroupBox(tr("Names"));
    /* Use two QFormLayouts side by side to benefit from better
     * styling of those forms that one would get from a QGridLayout: */
    QHBoxLayout *namesLayout = new QHBoxLayout;
    QFormLayout *namesLayout1 = new QFormLayout;  // 1st column
    QFormLayout *namesLayout2 = new QFormLayout;  // 2nd column
    {
      namesLayout1->addRow(tr("Alert unique name:"), descTitle);
      namesLayout1->addRow(tr("Optional Identifier:"), id);
      namesLayout2->addRow(tr("Text when firing:"), descFiring);
      namesLayout2->addRow(tr("Text when recovering:"), descRecovery);
      namesLayout->addLayout(namesLayout1);
      namesLayout->addLayout(namesLayout2);
    }
    namesBox->setLayout(namesLayout);
    outerLayout->addWidget(namesBox);

    // The notification condition
    QGroupBox *condition = new QGroupBox(tr("Main Condition"));
    QHBoxLayout *conditionLayout = new QHBoxLayout;
    {
      QFormLayout *metricForm = new QFormLayout;
      {
        QSizePolicy policy = source->sizePolicy();
        policy.setVerticalStretch(1);
        policy.setHorizontalStretch(2);
        source->setSizePolicy(policy);
        metricForm->addRow(tr("Metric:"), source);
        metricForm->addWidget(inexistantSourceError);
        metricForm->addWidget(mustSelectAField);
      }
      conditionLayout->addLayout(metricForm);

      // TODO: WHERE

      QFormLayout *limitLayout = new QFormLayout;
      {
        thresholdIsMax = new QRadioButton(tr("max"));
        thresholdIsMin = new QRadioButton(tr("min"));
        thresholdIsMax->setChecked(true);
        QHBoxLayout *minMaxLayout = new QHBoxLayout;
        minMaxLayout->addWidget(thresholdIsMax);
        minMaxLayout->addWidget(thresholdIsMin);
        minMaxLayout->addWidget(threshold);
        QWidget *minMaxBox = new QWidget;
        minMaxBox->setLayout(minMaxLayout);
        limitLayout->addRow(tr("Threshold:"), minMaxBox);
        limitLayout->addRow(tr("Hysteresis (%):"), hysteresis);
        limitLayout->addRow(tr("Measurements (%):"), percentage);
        limitLayout->addRow(tr("During the last (secs):"), duration);
      }
      conditionLayout->addLayout(limitLayout);
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
      description->setAlignment(Qt::AlignCenter);
    }
    descriptionBox->setLayout(descriptionBoxLayout);
    outerLayout->addWidget(descriptionBox);

    // Final
    outerLayout->addWidget(isEnabled);
  }
  setLayout(outerLayout);

  /* The values will be read from the various widgets when the OCaml value
   * is extracted from the form, yet we want to update the textual description
   * of the alert at every change: */
  connect(source, &NameTreeView::selectedChanged,
          this, &AlertInfoV1Editor::updateDescription);
  connect(thresholdIsMax, &QRadioButton::toggled,
          this, &AlertInfoV1Editor::updateDescription);
  connect(threshold, &QLineEdit::textChanged,
          this, &AlertInfoV1Editor::updateDescription);
  connect(hysteresis, &QLineEdit::textChanged,
          this, &AlertInfoV1Editor::updateDescription);
  connect(duration, &QLineEdit::textChanged,
          this, &AlertInfoV1Editor::updateDescription);
  connect(percentage, &QLineEdit::textChanged,
          this, &AlertInfoV1Editor::updateDescription);
  connect(id, &QLineEdit::textChanged,
          this, &AlertInfoV1Editor::updateDescription);
  connect(descTitle, &QLineEdit::textChanged,
          this, &AlertInfoV1Editor::updateDescription);
  connect(descFiring, &QLineEdit::textChanged,
          this, &AlertInfoV1Editor::updateDescription);
  connect(descRecovery, &QLineEdit::textChanged,
          this, &AlertInfoV1Editor::updateDescription);
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
  percentage->setEnabled(enabled);
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
    /* FIXME: scrollTo is supposed to expand collapsed parents but does
     * not properly (maybe because of a bug in NamesTree::parent(), so for
     * now let's just expand everything: */
    source->expandAll();
    source->scrollTo(index);
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
      100 * abs(v1.recovery - v1.threshold) /
      fmax(abs(v1.recovery), abs(v1.threshold));
  hysteresis->setText(QString::number(h));

  duration->setText(QString::number(v1.duration));

  percentage->setText(QString::number(100. * v1.ratio));

  timeStep = v1.timeStep; // just preserve that for now

  id->setText(QString::fromStdString(v1.id));

  descTitle->setText(QString::fromStdString(v1.descTitle));

  descFiring->setText(QString::fromStdString(v1.descFiring));

  descRecovery->setText(QString::fromStdString(v1.descRecovery));

  return true;
}

std::string const AlertInfoV1Editor::getTable() const
{
  NamesTree const *model = static_cast<NamesTree const *>(source->model());
  std::pair<std::string, std::string> const path =
    model->pathOfIndex(source->currentIndex());
  return path.first.empty() ? table : path.first;
}

std::string const AlertInfoV1Editor::getColumn() const
{
  NamesTree const *model = static_cast<NamesTree const *>(source->model());
  std::pair<std::string, std::string> const path =
    model->pathOfIndex(source->currentIndex());
  return path.second.empty() ? column : path.second;
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

void AlertInfoV1Editor::updateDescription() const
{
  std::string const table = getTable();
  std::string const column = getColumn();
  bool const has_table = table.length() > 0;
  bool const has_column = has_table && column.length() > 0;
  bool const has_threshold = threshold->hasAcceptableInput();
  bool const has_hysteresis = hysteresis->hasAcceptableInput();
  bool const has_duration = duration->hasAcceptableInput();
  bool const has_percentage = percentage->hasAcceptableInput();
  bool const has_where = false;
  bool const has_having = false; // TODO
  double const threshold_val = threshold->text().toDouble();
  double const hysteresis_val = hysteresis->text().toDouble();
  double const margin = 0.01 * hysteresis_val * threshold_val;
  double const recovery =
    thresholdIsMax->isChecked() ? threshold_val - margin :
                                  threshold_val + margin;
  double const percentage_val = percentage->text().toDouble();
  double const duration_val = duration->text().toDouble();
  if (percentage_val >= 100. && duration_val == 0) {
    description->setText(tr(
      "Fire notification \"%1%2\" when %3%4%5 is %6 %7%10,\n"
      "and recover when back %11 %12").
      arg(descTitle->text()).
      arg(descTitle->hasAcceptableInput() ? QString() : QString("…")).
      arg(has_table ? QString::fromStdString(table) : QString("…")).
      arg(has_column ? QString("/") + QString::fromStdString(column) :
                         (has_table ? QString("…") : QString())).
      arg(has_where ?
          QString(" for ") + QString("TODO") : QString()).
      arg(thresholdIsMax->isChecked() ?  tr("above") : tr("below")).
      arg(has_threshold ? threshold->text() : QString("…")).
      arg(has_having ? QString(" if ") + QString("TODO") : QString()).
      arg(thresholdIsMax->isChecked() ?  tr("below") : tr("above")).
      arg(has_threshold && has_hysteresis ?
        QString::number(recovery) : QString("…")));
  } else {
    description->setText(tr(
      "Fire notification \"%1%2\" when %3%4%5 is %6 %7 for %8% of the time "
      "during the last %9%10,\nand recover when back %11 %12").
      arg(descTitle->text()).
      arg(descTitle->hasAcceptableInput() ? QString() : QString("…")).
      arg(has_table ? QString::fromStdString(table) : QString("…")).
      arg(has_column ? QString("/") + QString::fromStdString(column) :
                         (has_table ? QString("…") : QString())).
      arg(has_where ?
          QString(" for ") + QString("TODO") : QString()).
      arg(thresholdIsMax->isChecked() ?  tr("above") : tr("below")).
      arg(has_threshold ? threshold->text() : QString("…")).
      arg(has_percentage ?  QString::number(percentage_val) : QString("…")).
      arg(has_duration ?  stringOfDuration(duration_val) : QString("…")).
      arg(has_having ? QString(" if ") + QString("TODO") : QString()).
      arg(thresholdIsMax->isChecked() ?  tr("below") : tr("above")).
      arg(has_threshold && has_hysteresis ?
        QString::number(recovery) : QString("…")));
  }
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
