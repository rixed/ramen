#include <cmath>
#include <QCheckBox>
#include <QCompleter>
#include <QDebug>
#include <QDoubleValidator>
#include <QFormLayout>
#include <QGroupBox>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QListView>
#include <QPushButton>
#include <QRadioButton>
#include <QRegularExpressionValidator>
#include <QStringListModel>
#include <QVBoxLayout>
#include "confValue.h"
#include "FilterEditor.h"
#include "NamesTree.h"
#include "PopupListView.h"
#include "misc.h"

#include "AlertInfoEditor.h"

static bool const verbose(false);

NameTreeView::NameTreeView(QWidget *parent) :
  QTreeView(parent)
{
  setUniformRowHeights(true);
  setHeaderHidden(true);
  setSelectionMode(QAbstractItemView::SingleSelection);
}

void NameTreeView::currentChanged(
  QModelIndex const &current, QModelIndex const &previous)
{
  QAbstractItemView::currentChanged(current, previous);
  emit selectedChanged(current);
}

AlertInfoEditor::AlertInfoEditor(QWidget *parent) :
  AtomicWidget(parent)
{
  source = new NameTreeView;
  // TODO: restrict to numerical fields
  // QTreeView to select the parent function field (aka "table" + "column")
  source->setModel(NamesTree::globalNamesTreeAnySites);
  connect(source, &NameTreeView::selectedChanged,
          this, &AlertInfoEditor::checkSource);
  connect(source, &NameTreeView::selectedChanged,
          this, &AlertInfoEditor::inputChanged);
  /* connect(source->model(), &NamesTree::rowsInserted,
          source, &NameTreeView::expand);*/

  /* The text is reset with the proper table/column name when an
   * error is detected: */
  inexistantSourceError = new QLabel;
  inexistantSourceError->hide();
  inexistantSourceError->setStyleSheet("color: red;");

  mustSelectAField = new QLabel(tr("Please select a field name"));

  isEnabled = new QCheckBox(tr("enabled"));
  isEnabled->setChecked(true);
  connect(isEnabled, &QCheckBox::stateChanged,
          this, &AlertInfoEditor::inputChanged);

  // TODO: list of FilterEditors rather
  where = new FilterEditor;
  connect(where, &FilterEditor::inputChanged,
          this, &AlertInfoEditor::inputChanged);

  autoGroupBy = new QCheckBox(tr("automatic"));
  autoGroupBy->setChecked(true);
  connect(autoGroupBy, &QCheckBox::stateChanged,
          this, &AlertInfoEditor::inputChanged);
  connect(autoGroupBy, &QCheckBox::stateChanged,
          this, &AlertInfoEditor::toggleAutoGroupBy);
  tableFields = new QStringListModel;
  // Must set the parent explicitly as it is not in the layout:
  groupBy = new PopupListView(this);
  groupBy->setModel(tableFields);
  groupBy->setSelectionMode(QAbstractItemView::MultiSelection);
  openGroupBy = new QPushButton;
  connect(groupBy->selectionModel(), &QItemSelectionModel::selectionChanged,
          this, &AlertInfoEditor::inputChanged);
  connect(source, &NameTreeView::selectedChanged,
          this, &AlertInfoEditor::checkGroupBy);

  having = new FilterEditor;
  connect(having, &FilterEditor::inputChanged,
          this, &AlertInfoEditor::inputChanged);

  threshold = new QLineEdit;
  threshold->setValidator(new QDoubleValidator);
  connect(threshold, &QLineEdit::textChanged,
          this, &AlertInfoEditor::inputChanged);
  hysteresis = new QLineEdit("10");
  hysteresis->setValidator(new QDoubleValidator(0., 100., 5));
  hysteresis->setPlaceholderText(tr("% of the value magnitude"));
  connect(hysteresis, &QLineEdit::textChanged,
          this, &AlertInfoEditor::inputChanged);
  duration = new QLineEdit;
  duration->setValidator(new QDoubleValidator(0., std::numeric_limits<double>::max(), 5)); // TODO: DurationValidator
  duration->setPlaceholderText(tr("duration"));
  connect(duration, &QLineEdit::textChanged,
          this, &AlertInfoEditor::inputChanged);
  percentage = new QLineEdit("100");
  percentage->setValidator(new QDoubleValidator(0., 100., 5));
  percentage->setPlaceholderText(tr("% of past measures"));
  connect(percentage, &QLineEdit::textChanged,
          this, &AlertInfoEditor::inputChanged);
  timeStep = new QLineEdit;
  timeStep->setValidator(new QDoubleValidator(0., 999999., 6));
  timeStep->setPlaceholderText(tr("seconds"));
  id = new QLineEdit;

  descTitle = new QLineEdit;
  /* At least one char that's not a space. Beware that validators
   * automatically anchor the pattern: */
  QRegularExpression nonEmpty(".*\\S+.*");
  descTitle->setValidator(new QRegularExpressionValidator(nonEmpty));
  connect(descTitle, &QLineEdit::textChanged,
          this, &AlertInfoEditor::inputChanged);
  descFiring = new QLineEdit;
  descFiring->setPlaceholderText(tr("alert!"));
  connect(descFiring, &QLineEdit::textChanged,
          this, &AlertInfoEditor::inputChanged);
  descRecovery = new QLineEdit;
  descRecovery->setPlaceholderText(tr("recovered"));
  connect(descRecovery, &QLineEdit::textChanged,
          this, &AlertInfoEditor::inputChanged);

  // TODO: List of tops/carry rather
  top = new QLineEdit;
  top->setPlaceholderText(tr("feature major contributors"));
  carryFields = new QLineEdit;
  carryFields->setPlaceholderText(tr("field to carry along"));
  // TODO: CarryCsts

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
        metricForm->addRow(tr("Time Step:"), timeStep);
        metricForm->addRow(tr("Where:"), where);
      }
      conditionLayout->addLayout(metricForm);

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
        limitLayout->addRow(tr("Hysteresis:"), hysteresis);
        limitLayout->addRow(tr("Measurements (%):"), percentage);
        limitLayout->addRow(tr("During the last (secs):"), duration);
        QHBoxLayout *groupByGroup = new QHBoxLayout;
        groupByGroup->addWidget(autoGroupBy);
        groupByGroup->addWidget(openGroupBy);
        limitLayout->addRow(tr("Group By:"), groupByGroup);
        /* Display the list below that button as a popup; */
        connect(openGroupBy, &QPushButton::clicked,
                this, &AlertInfoEditor::toggleGroupByView);
        limitLayout->addRow(tr("Having:"), having);
      }
      conditionLayout->addLayout(limitLayout);
    }
    condition->setLayout(conditionLayout);
    outerLayout->addWidget(condition);

    // Additional information to add to alerts:
    QGroupBox *addFieldsBox = new QGroupBox(tr("Attach additional fields"));
    QHBoxLayout *addFieldsBoxLayout = new QHBoxLayout;
    {
      addFieldsBoxLayout->addWidget(new QLabel(tr("Value of:")));
      addFieldsBoxLayout->addWidget(carryFields);
      addFieldsBoxLayout->addStretch();
      addFieldsBoxLayout->addWidget(new QLabel(tr("Breakdown of:")));
      addFieldsBoxLayout->addWidget(top);
    }
    addFieldsBox->setLayout(addFieldsBoxLayout);
    outerLayout->addWidget(addFieldsBox);

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

  QWidget *widget = new QWidget;
  widget->setLayout(outerLayout);
  relayoutWidget(widget);

  updateGroupByLabel();

  /* The values will be read from the various widgets when the OCaml value
   * is extracted from the form, yet we want to update the textual description
   * of the alert at every change: */
  connect(source, &NameTreeView::selectedChanged,
          this, &AlertInfoEditor::updateDescription);
  /* When a new table is selected the possible LHS of where and having has
   * to adapt: */
  connect(source, &NameTreeView::selectedChanged,
          this, &AlertInfoEditor::updateFilters);
  /* Update the openGroupBy button label whenever the selection changes: */
  connect(groupBy->selectionModel(), &QItemSelectionModel::selectionChanged,
          this, &AlertInfoEditor::updateGroupByLabel);
  connect(thresholdIsMax, &QRadioButton::toggled,
          this, &AlertInfoEditor::updateDescription);
  connect(threshold, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(hysteresis, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(duration, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(percentage, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(id, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(descTitle, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(descFiring, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(descRecovery, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(where, &FilterEditor::inputChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(having, &FilterEditor::inputChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(timeStep, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(top, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
  connect(carryFields, &QLineEdit::textChanged,
          this, &AlertInfoEditor::updateDescription);
}

void AlertInfoEditor::updateGroupByLabel()
{
  QStringList const lst { getGroupByQStrings() };

  openGroupBy->setText(
    lst.isEmpty() ?
      tr("select fields") :
      abbrev(50, lst.join(", ")));
}

void AlertInfoEditor::toggleGroupByView()
{
  if (groupBy->isVisible()) {
    groupBy->hide();
  } else {
    QPoint const pos { openGroupBy->mapToGlobal(QPoint(0, openGroupBy->height())) };
    groupBy->move(pos.x(), pos.y());
    groupBy->show();
  }
}

void AlertInfoEditor::toggleAutoGroupBy(int state)
{
  bool const manualGroupBy { state == Qt::Unchecked };
  openGroupBy->setEnabled(manualGroupBy);
  groupBy->setEnabled(manualGroupBy);
  if (! manualGroupBy) groupBy->hide();
}

void AlertInfoEditor::setEnabled(bool enabled)
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
  timeStep->setEnabled(enabled);
  where->setEnabled(enabled);
  autoGroupBy->setEnabled(enabled);
  bool const manualGroupBy { !autoGroupBy->isChecked() };
  openGroupBy->setEnabled(enabled && manualGroupBy);
  groupBy->setEnabled(enabled && manualGroupBy);
  having->setEnabled(enabled);
  top->setEnabled(enabled);
  carryFields->setEnabled(enabled);

  if (enabled) {
    checkSource(source->currentIndex());
  } else {
    mustSelectAField->hide();
  }
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

  AlertInfo *info = static_cast<AlertInfo *>(alert->info);

  /* Source:
   * Look for the name "$table/$column" and select it, but
   * also save the table and column names in case they are not
   * known (for instance if the program is not running (yet)). */
  _table = info->table;
  _column = info->column;
  NamesTree *model = static_cast<NamesTree *>(source->model());
  std::string const path(info->table + "/" + info->column);
  QModelIndex index(model->find(path));
  if (index.isValid()) {
    source->setCurrentIndex(index);
    /* FIXME: scrollTo is supposed to expand collapsed parents but does
     * not properly (maybe because of a bug in NamesTree::parent(), so for
     * now let's just expand everything: */
    source->expandAll();
    source->scrollTo(index);
    checkSource(index);
    checkGroupBy(index);
  } else {
    if (verbose)
      qDebug() << "Cannot find field" << QString::fromStdString(path);
    inexistantSourceError->setText(
      tr("Field %1/%2 does not exist")
      .arg(QString::fromStdString(info->table))
      .arg(QString::fromStdString(info->column)));
    inexistantSourceError->show();
  }

  isEnabled->setChecked(info->isEnabled);

  // TODO: support multiple where/having
  if (info->where.empty()) {
    where->clear();
  } else {
    where->setValue(info->where.front());
  }

  _groupBy = info->groupBy;
  QItemSelectionModel *selModel { groupBy->selectionModel() };
  selModel->clearSelection();
  if (info->groupBy) {
    autoGroupBy->setChecked(false);
    toggleAutoGroupBy(Qt::Unchecked);
    QStringList const fields { tableFields->stringList() };
    for (std::string const &s_ : *info->groupBy) {
      QString const s { QString::fromStdString(s_) };
      int const row { fields.indexOf(s) };
      if (row >= 0) {
        selModel->select(tableFields->index(row), QItemSelectionModel::Select);
      } else {
        qCritical() << "AlertInfoEditor::setValue: Cannot find field " << s;
      }
    }
  } else {
    autoGroupBy->setChecked(true);
    toggleAutoGroupBy(Qt::Checked);
  }
  updateGroupByLabel();

  if (info->having.empty()) {
    having->clear();
  } else {
    having->setValue(info->having.front());
  }

  // FIXME: threshold can optionally be a baseline now:
  ConstantThreshold const *constThreshold {
    dynamic_cast<ConstantThreshold const *>(info->threshold.get()) };
  if (constThreshold) {
    threshold->setText(QString::number(constThreshold->v));
  } else {
    threshold->setText("TODO"); //QString::number(info->threshold));
  }

  // Display the hysteresis in absolute value as defined
  hysteresis->setText(QString::number(info->hysteresis));

  duration->setText(QString::number(info->duration));

  percentage->setText(QString::number(100. * info->ratio));

  timeStep->setText(
    info->timeStep > 0 ? QString::number(info->timeStep) : QString());

  id->setText(QString::fromStdString(info->id));

  descTitle->setText(QString::fromStdString(info->descTitle));

  descFiring->setText(QString::fromStdString(info->descFiring));

  descRecovery->setText(QString::fromStdString(info->descRecovery));

  if (info->tops.empty()) {
    top->clear();
  } else {
    top->setText(QString::fromStdString(info->tops.front()));
  }
  if (info->carryFields.empty()) {
    carryFields->clear();
  } else {
    carryFields->setText(QString::fromStdString(info->carryFields.front()));
  }

  return true;
}

std::string const AlertInfoEditor::getTable() const
{
  NamesTree const *model = static_cast<NamesTree const *>(source->model());
  std::pair<std::string, std::string> const path =
    model->pathOfIndex(source->currentIndex());
  return path.first.empty() ? _table : path.first;
}

std::string const AlertInfoEditor::getColumn() const
{
  NamesTree const *model = static_cast<NamesTree const *>(source->model());
  std::pair<std::string, std::string> const path =
    model->pathOfIndex(source->currentIndex());
  return path.second.empty() ? _column : path.second;
}

QStringList const AlertInfoEditor::getGroupByQStrings() const
{
  QItemSelectionModel const *selModel { groupBy->selectionModel() };
  QModelIndexList selIdxs { selModel->selectedRows() };
  QStringList lst;

  for (int i = 0; i < selIdxs.length(); i++) {
    lst += tableFields->data(selIdxs[i]).toString();
  }

  return lst;
}

std::optional<std::set<std::string>> const AlertInfoEditor::getGroupBy() const
{
  if (autoGroupBy->isChecked())
    return std::nullopt;

  QStringList const lst { getGroupByQStrings() };
  std::set<std::string> ret;

  for (int i = 0; i < lst.length(); i++)
    ret.emplace(lst[i].toStdString());

  return ret;
}

std::shared_ptr<conf::Value const> AlertInfoEditor::getValue() const
{
  // FIXME: simplify
  std::unique_ptr<AlertInfo> info(
    std::make_unique<AlertInfo>(this));

  return std::shared_ptr<conf::Value const>(
    new conf::Alert(std::move(info)));
}

/* This is called each time we change or set the source to some value: */
void AlertInfoEditor::checkSource(QModelIndex const &current) const
{
  inexistantSourceError->hide();
  NamesTree *model = static_cast<NamesTree *>(source->model());
  mustSelectAField->setVisible(! model->isField(current));
}

void AlertInfoEditor::checkGroupBy(QModelIndex const &current)
{
  NamesTree *model = static_cast<NamesTree *>(source->model());
  if (! model->isField(current)) return;

  /* Get the list of fields in this table, expunge from groupBy all unknown
   * fields, and if anything changed reset the model content and openGroupBy's
   * label: */
  QModelIndex const tableIdx { current.parent() };
  bool changedSel { false };
  QStringList const fieldList { tableFields->stringList() };
  if (verbose)
    qDebug() << "checkGroupBy: syncing" << model->rowCount(tableIdx)
             << "rows from the model with" << fieldList.length()
             << "rows from the groupBy list";
  int fieldListIdx { 0 }; // Iterates over the groupBy QStringListModel...
  int row { 0 }; // ...whereas row iterates over the NamesTree model
  while (row < model->rowCount(tableIdx) || fieldListIdx < fieldList.length()) {
    QString field;
    int cmp;
    if (row >= model->rowCount(tableIdx))
      // That trailing fieldList entry must be removed
      goto remove_extra;
    field = model->data(model->index(row, 0, tableIdx)).toString();
    if (fieldListIdx >= fieldList.length())
      // Must add that missing row into fieldList
      goto add_missing;
    // If we have both entries, compare them:
    cmp = fieldList[fieldListIdx].compare(field);
    if (cmp < 0) {  // fieldList[fieldListIdx] must be removed from tableFields
remove_extra:
      changedSel = changedSel ||
                   groupBy->selectionModel()->isRowSelected(row, QModelIndex());
      tableFields->removeRows(row, 1);
      // Keep trying with next fieldListIdx
      fieldListIdx ++;
    } else if (cmp > 0) { // field must be inserted here
add_missing:
      tableFields->insertRows(row, 1);
      tableFields->setData(tableFields->index(row, 0), field);
      // Keep looking for this fieldListIdx
      row ++;
    } else { // cmp == 0, move to next row
      fieldListIdx ++;
      row ++;
    }
  }

  // If we changed the *selection* then update openGroupBy's label: */
  if (changedSel) updateGroupByLabel();
}

void AlertInfoEditor::updateDescription()
{
  std::string const table = getTable();
  std::string const column = getColumn();
  bool const has_table = table.length() > 0;
  bool const has_column = has_table && column.length() > 0;
  bool const has_threshold = threshold->hasAcceptableInput();
  bool const has_hysteresis = hysteresis->hasAcceptableInput();
  bool const has_duration = duration->hasAcceptableInput();
  bool const has_timeStep = timeStep->hasAcceptableInput();
  bool const has_percentage = percentage->hasAcceptableInput();
  QString const where_desc =
    where->description("\n(considering only values which ", "), ");
  QString const having_desc =
    having->description("\nwhenever the aggregated ", ", ");
  double const threshold_val = threshold->text().toDouble();
  double const hysteresis_val = hysteresis->text().toDouble();
  double const recovery = threshold_val + hysteresis_val;
  double const percentage_val = percentage->text().toDouble();
  double const duration_val = duration->text().toDouble();
  QString const timeStep_text {
    has_timeStep ?
      tr(" (aggregated by %1)").arg(
        stringOfDuration(timeStep->text().toDouble())) :
      "" };
  QString const topFields {
    top->hasAcceptableInput() ?
      tr("the breakdown of the top contributing %1").arg(top->text()) : "" };
  QString const carryFieldsStr {
    carryFields->hasAcceptableInput() ?
      tr("the value of %1").arg(carryFields->text()) : "" };
  /* TODO: CarryCsts */
  QString const attachedFields {
    !topFields.isEmpty() && !carryFieldsStr.isEmpty() ?
      tr("%1 and %2").arg(carryFieldsStr).arg(topFields) :
      !topFields.isEmpty() ? topFields : carryFieldsStr };
  QString const attachedFields_text {
    !attachedFields.isEmpty() ?
      tr("In addition to the tracked metric, the notification will also "
         "feature %1").arg(attachedFields) : "" };
  if (percentage_val >= 100. && duration_val == 0) {
    description->setText(tr(
      "Fire notification \"%1%2\" when %3%4%5 is %6 %7%8%9\n"
      "and recover when back %10 %11.\n%12").
      arg(descTitle->text()).
      arg(descTitle->hasAcceptableInput() ? QString() : QString("…")).
      arg(has_table ? QString::fromStdString(table) : QString("…")).
      arg(has_column ? QString("/") + QString::fromStdString(column) :
                         (has_table ? QString("…") : QString())).
      arg(timeStep_text).
      arg(thresholdIsMax->isChecked() ?  tr("above") : tr("below")).
      arg(has_threshold ? threshold->text() : QString("…")).
      arg(where_desc).
      arg(having_desc).
      arg(thresholdIsMax->isChecked() ?  tr("below") : tr("above")).
      arg(has_threshold && has_hysteresis ?
        QString::number(recovery) : QString("…")).
      arg(attachedFields_text));
  } else if (percentage_val >= 100.) {
    description->setText(tr(
      "Fire notification \"%1%2\" when %3%4%5 is consistently %6 %7\n"
      "for the last %8%9%10\n"
      "and recover when back %11 %12.\n%13").
      arg(descTitle->text()).
      arg(descTitle->hasAcceptableInput() ? QString() : QString("…")).
      arg(has_table ? QString::fromStdString(table) : QString("…")).
      arg(has_column ? QString("/") + QString::fromStdString(column) :
                         (has_table ? QString("…") : QString())).
      arg(timeStep_text).
      arg(thresholdIsMax->isChecked() ? tr("above") : tr("below")).
      arg(has_threshold ? threshold->text() : QString("…")).
      arg(has_duration ? stringOfDuration(duration_val) : QString("…")).
      arg(where_desc).
      arg(having_desc).
      arg(thresholdIsMax->isChecked() ? tr("below") : tr("above")).
      arg(has_threshold && has_hysteresis ?
        QString::number(recovery) : QString("…")).
      arg(attachedFields_text));
  } else {
    description->setText(tr(
      "Fire notification \"%1%2\" when %3%4%5 is %6 %7%8\nfor at least %9% "
      "of the time during the last %10%11\nand recover when back %12 %13.\n"
      "%14").
      arg(descTitle->text()).
      arg(descTitle->hasAcceptableInput() ? QString() : QString("…")).
      arg(has_table ? QString::fromStdString(table) : QString("…")).
      arg(has_column ? QString("/") + QString::fromStdString(column) :
                         (has_table ? QString("…") : QString())).
      arg(timeStep_text).
      arg(thresholdIsMax->isChecked() ? tr("above") : tr("below")).
      arg(has_threshold ? threshold->text() : QString("…")).
      arg(where_desc).
      arg(has_percentage ? QString::number(percentage_val) : QString("…")).
      arg(has_duration ? stringOfDuration(duration_val) : QString("…")).
      arg(having_desc).
      arg(thresholdIsMax->isChecked() ? tr("below") : tr("above")).
      arg(has_threshold && has_hysteresis ?
        QString::number(recovery) : QString("…")).
      arg(attachedFields_text));
  }
}

/* Check that this index is a field and if so reset the where and filter
 * function with this field parent: */
void AlertInfoEditor::updateFilters(QModelIndex const &current)
{
  if (! current.isValid()) return;

  NamesTree const *model = static_cast<NamesTree const *>(source->model());
  if (! model->isField(current)) {
    if (verbose)
      qDebug() << "AlertInfoEditor: selected source is not in a field";
    return;
  }

  QModelIndex const parent(current.parent());
  if (verbose)
    qDebug() << "AlertInfoEditor: selecting parent" << model->data(parent, 0);
  if (! parent.isValid()) {
    qCritical() << "AlertInfoEditor: field has no parent?!";
    return;
  }

  where->setFunction(parent);
  having->setFunction(parent);

  if (topCompleter) topCompleter->deleteLater();
  topCompleter =
    new NamesCompleter(NamesTree::globalNamesTreeAnySites, this, parent);
  topCompleter->setCompletionMode(QCompleter::UnfilteredPopupCompletion);
  top->setCompleter(topCompleter);

  if (carryFieldsCompleter) carryFieldsCompleter->deleteLater();
  carryFieldsCompleter =
    new NamesCompleter(NamesTree::globalNamesTreeAnySites, this, parent);
  carryFieldsCompleter->setCompletionMode(QCompleter::UnfilteredPopupCompletion);
  carryFields->setCompleter(carryFieldsCompleter);
}

bool AlertInfoEditor::hasValidInput() const
{
  NamesTree const *model(static_cast<NamesTree *>(source->model()));
  if (!model->isField(source->currentIndex())) {
    if (verbose)
      qDebug() << "AlertInfoEditor: selected source is not a field";
    return false;
  }
  if (!threshold->hasAcceptableInput()) {
    if (verbose)
      qDebug() << "AlertInfoEditor: threshold invalid";
    return false;
  }
  if (!hysteresis->hasAcceptableInput()) {
    if (verbose)
      qDebug() << "AlertInfoEditor: hysteresis invalid";
    return false;
  }
  if (!duration->text().isEmpty() && !duration->hasAcceptableInput()) {
    if (verbose)
      qDebug() << "AlertInfoEditor: duration invalid";
    return false;
  }
  if (!timeStep->text().isEmpty() && !timeStep->hasAcceptableInput()) {
    if (verbose)
      qDebug() << "AlertInfoEditor: time-step invalid";
    return false;
  }
  if (!percentage->hasAcceptableInput()) {
    if (verbose)
      qDebug() << "AlertInfoEditor: percentage invalid";
    return false;
  }
  if (!descTitle->hasAcceptableInput()) {
    if (verbose)
      qDebug() << "AlertInfoEditor: name invalid";
    return false;
  }
  if (!where->isEmpty() && !where->hasValidInput()) {
    if (verbose)
      qDebug() << "AlertInfoEditor: where invalid";
    return false;
  }
  if (!having->isEmpty() && !having->hasValidInput()) {
    if (verbose)
      qDebug() << "AlertInfoEditor: having invalid";
    return false;
  }

  if (verbose)
    qDebug() << "AlertInfoEditor: is valid";

  return true;
}
