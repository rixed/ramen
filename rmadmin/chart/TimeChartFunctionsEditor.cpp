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

static bool const verbose { false };

TimeChartFunctionsEditor::TimeChartFunctionsEditor(QWidget *parent)
  : QWidget(parent)
{
  functions = new QToolBox(this);
  functions->setSizePolicy(QSizePolicy::Maximum, QSizePolicy::Minimum);

  // TODO: a globalGraphModelWithoutTopHalves
  GraphModel *graph(GraphModel::globalGraphModel);
  functionSelector = new FunctionSelector(graph);
  QPushButton *addButton = new QPushButton(tr("Add"));
  connect(addButton, &QPushButton::clicked,
          this, &TimeChartFunctionsEditor::addCurrentFunction);

  QHBoxLayout *fsLayout = new QHBoxLayout;
  fsLayout->addWidget(functionSelector, 1);
  fsLayout->addWidget(addButton, 0);
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(functions);
  layout->addLayout(fsLayout);
  setLayout(layout);
}

bool TimeChartFunctionsEditor::setValue(
  std::shared_ptr<conf::DashWidgetChart const> v)
{
  if (verbose)
    qDebug() << "TimeChartFunctionsEditor::setValue with" << v->sources.size()
             << "sources and" << v->axes.size() << "axes";

  /* Sources are ordered by name */

  size_t v_i(0); // index in v->sources
  int t_i(0); // index in items
  for (;
    v_i < v->sources.size() || t_i < functions->count();
    v_i++, t_i++
  ) {
    if (v_i >= v->sources.size()) {
      if (verbose) qDebug() << "extra function" << t_i << "is gone";
      allFieldsChanged(t_i);
      functions->removeItem(t_i--);
    } else if (t_i >= functions->count()) {
      conf::DashWidgetChart::Source const &src = v->sources[v_i];
      if (verbose)
        qDebug() << "appending new function at" << t_i << "for"
                 << QString::fromStdString(src.function);
      TimeChartFunctionEditor *e = addFunctionByName(
        src.site, src.program, src.function, true);
      e->setValue(src);
      functions->addItem(e, src.name);
    } else {
      int const c(v->sources[v_i].name.compare(functions->itemText(t_i)));
      if (c == 0) {
        if (verbose) qDebug() << "updating function" << t_i;
        TimeChartFunctionEditor *e =
          static_cast<TimeChartFunctionEditor *>(functions->widget(t_i));
        e->setValue(v->sources[v_i]);
      } else if (c < 0) {
        // v->source comes first
        conf::DashWidgetChart::Source const &src = v->sources[v_i];
        if (verbose)
          qDebug() << "inserting function at" << t_i << "for"
                   << QString::fromStdString(src.function);
        TimeChartFunctionEditor *e = addFunctionByName(
          src.site, src.program, src.function, true);
        e->setValue(src);
        (void)functions->insertItem(t_i, e, src.name);
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
  conf::DashWidgetChart::Source const &source(e->model->source);
  for (conf::DashWidgetChart::Column const &field : source.fields) {
    emit fieldChanged(source.site, source.program, source.function, field.name);
  }
}

void TimeChartFunctionsEditor::addCurrentFunction()
{
  FunctionItem *f(functionSelector->getCurrent());
  if (! f) {
    qDebug() << "Must select a function";
    return;
  }

  std::shared_ptr<Function> function(
    std::dynamic_pointer_cast<Function>(f->shared));
  if (! function) {
    qWarning() << "No such function";
    return;
  }

  addOrFocus(
    function->siteName.toStdString(),
    function->programName.toStdString(),
    function->name.toStdString(),
    true);
}

void TimeChartFunctionsEditor::addOrFocus(
  std::string const &site,
  std::string const &program,
  std::string const &function,
  bool customizable)
{
  QString const fqName(
    QString::fromStdString(site) + ":" +
    QString::fromStdString(program) + "/" +
    QString::fromStdString(function));

  int t_i;
  for (t_i = 0; t_i < functions->count(); t_i++) {
    int const c(fqName.compare(functions->itemText(t_i)));
    if (c == 0) {
      /* If this function is already in the list, just focus it: */
      functions->setCurrentIndex(t_i);
      return;
    } else if (c < 0) {
      break;
    }
  }

  /* Create a new function editor */
  if (verbose)
    qDebug() << "Insert new function at index" << t_i;
  TimeChartFunctionEditor *e = addFunctionByName(
    site, program, function, customizable);
  conf::DashWidgetChart::Source defaultSrc { site, program, function };
  e->setValue(defaultSrc);
  (void)functions->insertItem(t_i, e, fqName);
  functions->setCurrentIndex(t_i);
}

TimeChartFunctionEditor *TimeChartFunctionsEditor::addFunctionByName(
  std::string const &site,
  std::string const &program,
  std::string const &function,
  bool customizable)
{
  std::shared_ptr<Function const> const f(
    Function::find(
      QString::fromStdString(site),
      QString::fromStdString(program),
      QString::fromStdString(function)));

  if (! f) {
    qCritical() << "Customized function does not exist yet!";
    return nullptr;
  }

  if (verbose)
    qDebug() << "TimeChartFunctionsEditor: adding function"
             << f->fqName;

  TimeChartFunctionEditor *e = new TimeChartFunctionEditor(
    site, program, function, customizable);

  connect(e, &TimeChartFunctionEditor::fieldChanged,
          this, &TimeChartFunctionsEditor::fieldChanged);

  if (customizable) {
    connect(e, &TimeChartFunctionEditor::customizedFunction,
            this, &TimeChartFunctionsEditor::addCustomizedFunction);
  }

  return e;
}

void TimeChartFunctionsEditor::addCustomizedFunction(
  std::string const &site,
  std::string const &program,
  std::string const &function)
{
  if (verbose)
    qDebug() << "TimeChartFunctionsEditor: adding a customized function";
  addOrFocus(site, program, function, false);
}
