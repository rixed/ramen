#include <cassert>
#include <cstdlib>
#include <QCheckBox>
#include <QCompleter>
#include <QDebug>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QLineEdit>
#include <QModelIndex>
#include <QPushButton>
#include <QTableView>
#include <QVBoxLayout>
#include "chart/FactorsDelegate.h"
#include "chart/TimeChartAutomatonCustomize.h"
#include "chart/TimeChartFunctionFieldsModel.h"
#include "ColorDelegate.h"
#include "conf.h"
#include "confAutomaton.h"
#include "confRCEntry.h"
#include "confValue.h"
#include "FixedTableView.h"
#include "Menu.h"
#include "misc.h"
#include "RamenValue.h"
#include "Resources.h"
#include "RollButtonDelegate.h"
#include "SourcesWin.h"

#include "chart/TimeChartFunctionEditor.h"

static bool const verbose { false };

TimeChartFunctionEditor::TimeChartFunctionEditor(
  std::string const &site,
  std::string const &program,
  std::string const &function,
  bool customizable,
  QWidget *parent)
  : QWidget(parent)
{
  visible = new QCheckBox(tr("Visible"));
  visible->setChecked(true);

  if (customizable) {
    customize = new QPushButton(tr("Customize"));
    connect(customize, &QPushButton::clicked,
            this, &TimeChartFunctionEditor::wantCustomize);
  } else {
    customize = nullptr;
  }

  openSource = new QPushButton(tr("Source…"));
  connect(openSource, &QPushButton::clicked,
          this, &TimeChartFunctionEditor::wantSource);

  Resources *r = Resources::get();

  deleteButton = new QPushButton(r->deletePixmap, QString());
  connect(deleteButton, &QPushButton::clicked,
          this, &QWidget::deleteLater);

  model = new TimeChartFunctionFieldsModel(site, program, function);

  fields = new FixedTableView;
  fields->setModel(model);
  fields->setShowGrid(false);
  fields->setMinimumSize(80, 80);
  fields->resizeColumnsToContents();
  // Best thing after having all the editors open at once:
  fields->setEditTriggers(QAbstractItemView::AllEditTriggers);

  RollButtonDelegate *reprDelegate = new RollButtonDelegate;
  reprDelegate->addIcon(r->emptyIcon);
  reprDelegate->addIcon(r->lineChartIcon);
  reprDelegate->addIcon(r->stackedChartIcon);
  reprDelegate->addIcon(r->stackCenteredChartIcon);
  fields->setItemDelegateForColumn(
    TimeChartFunctionFieldsModel::ColRepresentation, reprDelegate);

  factorsDelegate = new FactorsDelegate(this);
  factorsDelegate->setColumns(model->factors);
  fields->setItemDelegateForColumn(
    TimeChartFunctionFieldsModel::ColFactors, factorsDelegate);

  ColorDelegate *colorDelegate = new ColorDelegate;
  fields->setItemDelegateForColumn(
    TimeChartFunctionFieldsModel::ColColor, colorDelegate);

  connect(model, &QAbstractTableModel::dataChanged,
          this, [this](QModelIndex const &topLeft,
                       QModelIndex const &bottomRight) {
    if (verbose)
      qDebug() << "model data changed from row" << topLeft.row()
               << "to" << bottomRight.row();
    // First reset the factors used by the delegate:
    factorsDelegate->setColumns(model->factors);
    // Then emit fieldChanged for every changed fields:
    int const lastRow = bottomRight.row();
    conf::DashWidgetChart::Source const &source = model->source;
    for (int row = topLeft.row(); row <= lastRow; row++) {
      /* Model row correspond to numericFields not source.fields! */
      if (row > model->numericFields.count()) {
        qCritical("TimeChartFunctionEditor: dataChanged on row %d but model "
                  "has only %d rows!", row, model->numericFields.count());
        break;
      }
      if (verbose)
        qDebug() << "TimeChartFunctionEditor: fieldChanged"
                 << model->numericFields[row];
      emit fieldChanged(source.site, source.program, source.function,
                        model->numericFields[row].toStdString());
    }
  });

  QHBoxLayout *topHBox = new QHBoxLayout;
  topHBox->setObjectName("topHBox");
  topHBox->addWidget(visible);
  topHBox->addStretch();
  if (customize) topHBox->addWidget(customize);
  topHBox->addWidget(openSource);
  topHBox->addWidget(deleteButton);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addLayout(topHBox);
  layout->addWidget(fields);
  setLayout(layout);
}

void TimeChartFunctionEditor::wantSource()
{
  if (! Menu::sourcesWin) return;

  std::string const sourceKeyPrefix(
    "sources/" + srcPathFromProgramName(model->source.program));
  if (verbose)
    qDebug() << "Show source of program" << QString::fromStdString(sourceKeyPrefix);

  Menu::sourcesWin->showFile(sourceKeyPrefix);
  Menu::openSourceEditor();
}

/* Customization is a multi step process:
 * 1. Create a new source that selects everything from that function;
 * 2. Once created, displays it in the editor;
 * 3. Once compiled, run it on this site only where the original run;
 * 4. Once the worker is received, substitute the plotted function with
 *    this custom one. */

void TimeChartFunctionEditor::wantCustomize()
{
  TimeChartAutomatonCustomize *automaton =
    new TimeChartAutomatonCustomize(
      model->source.site, model->source.program, model->source.function, this);

  connect(automaton, &TimeChartAutomatonCustomize::transitionTo,
          this, &TimeChartFunctionEditor::automatonTransition);

  std::shared_ptr<conf::RamenValueValue const> newSource(
    std::make_shared<conf::RamenValueValue>(
      new VString(
        QString::fromStdString(
          "DEFINE LAZY '" + automaton->customFunction + "' AS\n"
          "  SELECT\n"
          "    *\n" // TODO: rather list the fields explicitly, with their doc
          "  FROM '"
            + model->source.program + "/"
            + model->source.function + "' ON THIS SITE;\n"))));

  askNew(automaton->sourceKey, newSource);
}

void TimeChartFunctionEditor::automatonTransition(
  conf::Automaton *automaton_, size_t state,
  std::shared_ptr<conf::Value const> val)
{
  qInfo() << "TimeChartFunctionEditor: automaton entering state" << state;

  TimeChartAutomatonCustomize *automaton(
    dynamic_cast<TimeChartAutomatonCustomize *>(automaton_));
  if (!automaton) {
    qCritical() << "TimeChartFunctionEditor::automatonTransition:"
                   " not a TimeChartAutomatonCustomize?!";
    return;
  }

  switch (state) {
    case TimeChartAutomatonCustomize::WaitSource:
      assert(false);  // not transited _to_

    case TimeChartAutomatonCustomize::WaitInfo:
      qInfo() << "TimeChartFunctionEditor: displaying the customized source";
      Menu::sourcesWin->showFile(removeExt(automaton->sourceKey, '/'));
      Menu::openSourceEditor();
      break;

    case TimeChartAutomatonCustomize::WaitLockRC:
      qInfo() << "TimeChartFunctionEditor: locking the target "
                 "running configuration";
      {
        // Check that compilation worked
        std::shared_ptr<conf::SourceInfo const> info(
          std::dynamic_pointer_cast<conf::SourceInfo const>(val));
        if (!info) {
          qCritical() << "key" << QString::fromStdString(automaton->infoKey)
                      << "not a SourceInfo?!";
          automaton->deleteLater();
          return;
        }
        if (!info->errMsg.isEmpty()) {
          // We generated this source ourselves!
          qCritical() << "Cannot compile customization template:"
                      << info->errMsg;
          automaton->deleteLater();
          return;
        }
        // Lock target_config
        askLock("target_config");
      }
      break;

    case TimeChartAutomatonCustomize::WaitWorkerOrGraph:
      qInfo() << "TimeChartFunctionEditor: running that program";
      {
        std::shared_ptr<conf::TargetConfig const> rc(
          std::dynamic_pointer_cast<conf::TargetConfig const>(val));
        if (!rc) {
          qCritical() << "target_config not a TargetConfig?!";
          automaton->deleteLater();
          return;
        }
        std::shared_ptr<conf::TargetConfig> rc2(
          std::make_shared<conf::TargetConfig>(*rc));
        // Look at the RC for this function for inspiration:
        std::shared_ptr<conf::RCEntry> sourceEntry;
        for (std::pair<std::string const, std::shared_ptr<conf::RCEntry>> const &rce :
               rc->entries) {
          /* Ideally check also the site: */
          if (rce.first == model->source.program) {
            sourceEntry = rce.second;
            break;
          }
        }
        if (! sourceEntry) {
          qWarning() << "Cannot find program"
                     << QString::fromStdString(model->source.program)
                     << "in the RC file";
          automaton->deleteLater();
          return;
        }
        std::shared_ptr<conf::RCEntry> rce(
          std::make_shared<conf::RCEntry>(
            automaton->customProgram, true, sourceEntry->debug,
            sourceEntry->reportPeriod, sourceEntry->cwd,
            model->source.site, false));
        rc2->addEntry(rce);
        askSet("target_config", rc2);
      }
      break;

    case TimeChartAutomatonCustomize::Done:
      qInfo() << "TimeChartFunctionEditor: Substituting customized function "
                 "in the current chart.";
      emit customizedFunction(
        automaton->site, automaton->customProgram, automaton->customFunction);
      break;
  }
}

void TimeChartFunctionEditor::setEnabled(bool enabled)
{
  visible->setEnabled(enabled);
  if (customize) customize->setEnabled(enabled);
  openSource->setEnabled(enabled);
}

bool TimeChartFunctionEditor::setValue(
  conf::DashWidgetChart::Source const &source)
{
  if (verbose)
    qDebug() << "TimeChartFunctionEditor::setValue" << source;

  if (source.visible != visible->isChecked()) {
    visible->setChecked(source.visible);
  }
  model->setValue(source);

  /* Offer to delete (without confirmation dialog) a function as long as
   * it has no field drawn: */
  deleteButton->setEnabled(!model->hasSelection());

  return true;
}

conf::DashWidgetChart::Source TimeChartFunctionEditor::getValue() const
{
  conf::DashWidgetChart::Source source(model->source);
  source.visible = visible->isChecked();
  return source;
}
