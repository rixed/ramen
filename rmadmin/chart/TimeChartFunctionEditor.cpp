#include <QCheckBox>
#include <QCompleter>
#include <QDebug>
#include <QHeaderView>
#include <QLabel>
#include <QLineEdit>
#include <QModelIndex>
#include <QPushButton>
#include <QTableView>
#include <QVBoxLayout>
#include "chart/FactorsDelegate.h"
#include "chart/TimeChartFunctionFieldsModel.h"
#include "ColorDelegate.h"
#include "confValue.h"
#include "KTextEdit.h"
#include "misc.h"
#include "Resources.h"
#include "RollButtonDelegate.h"

#include "chart/TimeChartFunctionEditor.h"

static bool const verbose(false);

TimeChartFunctionEditor::TimeChartFunctionEditor(
  std::string const &site,
  std::string const &program,
  std::string const &function,
  QWidget *parent)
  : QWidget(parent),
    inlineFuncEdit(nullptr)
{
  visible = new QCheckBox(tr("Visible"));
  visible->setChecked(true);
  // TODO: inlineFuncEdit = ...

  model = new TimeChartFunctionFieldsModel(site, program, function);

  fields = new QTableView;
  fields->setModel(model);
  fields->setShowGrid(false);
  fields->setMinimumSize(80, 80);
  fields->resizeColumnsToContents();
  // Best thing after having all the editors open at once:
  fields->setEditTriggers(QAbstractItemView::AllEditTriggers);

  RollButtonDelegate *reprDelegate = new RollButtonDelegate;
  Resources *r = Resources::get();
  reprDelegate->addIcon(r->emptyIcon);
  reprDelegate->addIcon(r->lineChartIcon);
  reprDelegate->addIcon(r->stackedChartIcon);
  reprDelegate->addIcon(r->stackCenteredChartIcon);
  fields->setItemDelegateForColumn(
    TimeChartFunctionFieldsModel::ColRepresentation, reprDelegate);

  FactorsDelegate *factorsDelegate = new FactorsDelegate(model->factors);
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
    int const lastRow = bottomRight.row();
    conf::DashWidgetChart::Source const &source = model->source;
    for (int row = topLeft.row(); row <= lastRow; row++) {
      /* Model row correspond to numericFields now source.fields! */
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

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(visible);
  if (inlineFuncEdit) layout->addWidget(inlineFuncEdit);
  layout->addWidget(fields);
  setLayout(layout);
}

void TimeChartFunctionEditor::setEnabled(bool enabled)
{
  visible->setEnabled(enabled);
}

bool TimeChartFunctionEditor::setValue(
  conf::DashWidgetChart::Source const &source)
{
  if (source.visible != visible->isChecked()) {
    visible->setChecked(source.visible);
  }
  model->setValue(source);
  return true;
}

conf::DashWidgetChart::Source TimeChartFunctionEditor::getValue() const
{
  conf::DashWidgetChart::Source source(model->source);
  source.visible = visible->isChecked();
  return source;
}
