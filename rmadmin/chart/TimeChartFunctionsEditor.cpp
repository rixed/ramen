#include <QDebug>
#include <QHBoxLayout>
#include <QPushButton>
#include <QToolBox>
#include <QVBoxLayout>
#include "confValue.h"
#include "chart/TimeChartFunctionEditor.h"
#include "chart/TimeChartFunctionFieldsModel.h"
#include "FunctionItem.h"
#include "FunctionSelector.h"
#include "GraphModel.h"

#include "chart/TimeChartFunctionsEditor.h"

TimeChartFunctionsEditor::TimeChartFunctionsEditor(QWidget *parent)
  : QWidget(parent)
{
  functions = new QToolBox(this);

  GraphModel *graph(GraphModel::globalGraphModel);

  functionSelector = new FunctionSelector(graph);
  QPushButton *addButton = new QPushButton(tr("Add"));

  connect(addButton, &QPushButton::clicked,
          this, &TimeChartFunctionsEditor::addFunction);

  QHBoxLayout *fsLayout = new QHBoxLayout;
  fsLayout->addWidget(functionSelector, 1);
  fsLayout->addWidget(addButton, 0);
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(functions);
  layout->addLayout(fsLayout);
  setLayout(layout);
}

bool TimeChartFunctionsEditor::setValue(
  std::shared_ptr<conf::DashboardWidgetChart const> v)
{
  /* Sources are ordered by name */

  size_t v_i(0); // index in v->sources
  int t_i(0); // index in items
  for (;
    v_i < v->sources.size() || t_i < functions->count();
    v_i++, t_i++
  ) {
    if (v_i >= v->sources.size()) {
      allFieldsChanged(t_i);
      functions->removeItem(t_i--);
    } else if (t_i >= functions->count()) {
      conf::DashboardWidgetChart::Source const &src = v->sources[v_i];
      TimeChartFunctionEditor *e = new TimeChartFunctionEditor(
        src.site, src.program, src.function);
      connect(e, &TimeChartFunctionEditor::fieldChanged,
              this, &TimeChartFunctionsEditor::fieldChanged);
      e->setValue(src);
      functions->addItem(e, src.name);
    } else {
      int const c(v->sources[v_i].name.compare(functions->itemText(t_i)));
      if (c == 0) {
        TimeChartFunctionEditor *e =
          static_cast<TimeChartFunctionEditor *>(functions->widget(t_i));
        e->setValue(v->sources[v_i]);
      } else if (c < 0) {
        // v->source comes first
        conf::DashboardWidgetChart::Source const &src = v->sources[v_i];
        TimeChartFunctionEditor *e = new TimeChartFunctionEditor(
          src.site, src.program, src.function);
        connect(e, &TimeChartFunctionEditor::fieldChanged,
                this, &TimeChartFunctionsEditor::fieldChanged);
        e->setValue(src);
        (void)functions->insertItem(v_i, e, src.name);
      } else if (c > 0) {
        /* QToolBox item comes first. It must be a new function being edited,
         * and will be subsequently either saved or deleted when the form gets
         * submitted. For now, ignore it. */
      }
    }
  }

  return true;
}

void TimeChartFunctionsEditor::setEnabled(bool enabled)
{
  functions->setEnabled(enabled);
  functionSelector->setEnabled(enabled);
}

void TimeChartFunctionsEditor::allFieldsChanged(int tab_idx)
{
  TimeChartFunctionEditor *e =
    static_cast<TimeChartFunctionEditor *>(functions->widget(tab_idx));
  conf::DashboardWidgetChart::Source const &source(e->model->source);
  for (conf::DashboardWidgetChart::Column const &field : source.fields) {
    emit fieldChanged(source.site, source.program, source.function, field.name);
  }
}

void TimeChartFunctionsEditor::addFunction()
{
  FunctionItem *f(functionSelector->getCurrent());
  if (! f) {
    qDebug() << "Must select a function";
    return;
  }

  std::shared_ptr<Function> function(
    std::dynamic_pointer_cast<Function>(f->shared));

  if (! function) {
    qDebug() << "No such function";
    return;
  }

  /* If this function is already in the list, just focus it: */
  for (int t_i = 0; t_i < functions->count(); t_i++) {
    int const c(function->fqName.compare(functions->itemText(t_i)));
    if (c == 0) {
      functions->setCurrentIndex(t_i);
      return;
    } else if (c < 0) {
      /* Create a new function editor */
      TimeChartFunctionEditor *e = new TimeChartFunctionEditor(
        function->siteName.toStdString(),
        function->programName.toStdString(),
        function->name.toStdString());
      connect(e, &TimeChartFunctionEditor::fieldChanged,
              this, &TimeChartFunctionsEditor::fieldChanged);
      (void)functions->insertItem(t_i, e, function->fqName);
      functions->setCurrentIndex(t_i);
      return;
    }
  }
}
