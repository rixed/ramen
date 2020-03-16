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
#include "KTextEdit.h"
#include "chart/TimeChartColumnEditor.h"
#include "chart/TimeChartFunctionFieldsModel.h"
#include "confValue.h"
#include "misc.h"

#include "chart/TimeChartFunctionEditor.h"

static bool const verbose = false;

TimeChartFunctionEditor::TimeChartFunctionEditor(
  std::string const &site,
  std::string const &program,
  std::string const &function,
  QWidget *parent)
  : QWidget(parent),
    inlineFuncEdit(nullptr)
{
  visible = new QCheckBox(tr("Visible"));
  // TODO: inlineFuncEdit = ...

  model = new TimeChartFunctionFieldsModel(site, program, function);
  fields = new QTableView;
  fields->setModel(model);
  fields->setShowGrid(false);
  fields->setMinimumSize(80, 80);

  connect(model, &QAbstractTableModel::dataChanged,
          [this](QModelIndex const &topLeft, QModelIndex const &bottomRight) {
    if (verbose)
      qDebug() << "model data changed from row" << topLeft.row()
               << "to" << bottomRight.row();
    int const lastRow = bottomRight.row();
    conf::DashboardWidgetChart::Source const &source = model->source;
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
  conf::DashboardWidgetChart::Source const &source)
{
  if (source.visible != visible->isChecked()) {
    visible->setChecked(source.visible);
  }
  model->setValue(source);
  return true;
}

conf::DashboardWidgetChart::Source TimeChartFunctionEditor::getValue() const
{
  conf::DashboardWidgetChart::Source source(model->source);
  source.visible = visible->isChecked();
  return source;
}
