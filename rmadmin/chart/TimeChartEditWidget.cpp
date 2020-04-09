#include <QCheckBox>
#include <QDebug>
#include <QHBoxLayout>
#include <QLineEdit>
#include <QPushButton>
#include <QRadioButton>
#include <QTabWidget>
#include <QToolBox>
#include <QVBoxLayout>
#include "chart/TimeChartAxisEditor.h"
#include "chart/TimeChartFunctionEditor.h"
#include "chart/TimeChartFunctionFieldsModel.h"
#include "chart/TimeChartFunctionsEditor.h"
#include "chart/TimeChartOptionsEditor.h"
#include "confValue.h"
#include "conf.h"

#include "chart/TimeChartEditWidget.h"

static bool const verbose(false);

TimeChartEditWidget::TimeChartEditWidget(
  QPushButton *submitButton,
  QPushButton *cancelButton_,
  QWidget *parent)
  : AtomicWidget(parent),
    cancelButton(cancelButton_)
{
  optionsEditor = new TimeChartOptionsEditor(this);
  functionsEditor = new TimeChartFunctionsEditor;

  /* Axis editor must know when a new axis is referenced in a function
   * editor, to offer to edit it: */
  connect(functionsEditor, &TimeChartFunctionsEditor::fieldChanged,
          optionsEditor, &TimeChartOptionsEditor::updateAfterFieldChange);

  // Forward some signals from the editors to the chart:
  connect(optionsEditor, &TimeChartOptionsEditor::axisChanged,
          this, &TimeChartEditWidget::axisChanged);
  connect(functionsEditor, &TimeChartFunctionsEditor::fieldChanged,
          this, &TimeChartEditWidget::fieldChanged);

  QHBoxLayout *buttonsLayout = new QHBoxLayout;
  buttonsLayout->addStretch();
  if (cancelButton) buttonsLayout->addWidget(cancelButton);
  if (submitButton) buttonsLayout->addWidget(submitButton);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(optionsEditor, 0);
  layout->addWidget(functionsEditor, 1);
  layout->addLayout(buttonsLayout, 0);
  QWidget *w = new QWidget;
  w->setLayout(layout);
  relayoutWidget(w);
}

void TimeChartEditWidget::setEnabled(bool enabled)
{
  optionsEditor->setEnabled(enabled);
  functionsEditor->setEnabled(enabled);
}

bool TimeChartEditWidget::setValue(
  std::string const &key, std::shared_ptr<conf::Value const> v)
{
  std::shared_ptr<conf::DashWidgetChart const> conf =
    std::dynamic_pointer_cast<conf::DashWidgetChart const>(v);

  if (! conf) {
    /* Will happen if the underlying dashboard widget type changes.
     * Jut ignore it, the dashboard should notice and replace this
     * widget with a more appropriate one. */
    qWarning() << "TimeChartEditWidget::setValue: "
               << "passed value is not a conf::DashWidgetChart";
    return false;
  }

  if (verbose)
    qDebug() << "TimeChartEditWidget::setValue: setting a value with"
             << conf->axes.size() << "axes and"
             << conf->sources.size() << "sources.";

  optionsEditor->setValue(key, conf);
  functionsEditor->setValue(conf);

  return true;
}

std::shared_ptr<conf::Value const> TimeChartEditWidget::getValue() const
{
  conf::DashWidgetChart conf;  // start from an empty configuration

  conf.title = optionsEditor->title->text();
  qDebug() << "TimeChartEditWidget::getValue: title =" << conf.title;

  /* TODO: a signal from functionsEditor when a new axis is requested, that
   * would be connected to the AxisEditor.addAxis(where). */
  int const numAxes = optionsEditor->axes->count();
  for (int a_idx = 0; a_idx < numAxes; a_idx++) {
    TimeChartAxisEditor const *axisEditor =
      static_cast<TimeChartAxisEditor const *>(optionsEditor->axes->widget(a_idx));
    conf::DashWidgetChart::Axis const axisConf(axisEditor->getValue());
    conf.axes.push_back(axisConf);
  }

  int const numFunctions = functionsEditor->functions->count();
  for (int f_idx = 0; f_idx < numFunctions; f_idx++) {
    TimeChartFunctionEditor const *funcEditor =
      static_cast<TimeChartFunctionEditor const *>(
        functionsEditor->functions->widget(f_idx));
    conf.sources.push_back(funcEditor->getValue());
  }

  if (verbose)
    qDebug() << "TimeChartEditWidget::getValue: returning a value with"
             << conf.axes.size() << "axes and"
             << conf.sources.size() << "sources.";

  return std::static_pointer_cast<conf::Value const>(
    std::make_shared<conf::DashWidgetChart>(conf));
}

int TimeChartEditWidget::axisCountOnSide(bool left) const
{
  int count = 0;
  int const tabCount = optionsEditor->axes->count();
  for (int i = 0; i < tabCount; i++) {
    TimeChartAxisEditor const *axisEditor =
      static_cast<TimeChartAxisEditor const *>(optionsEditor->axes->widget(i));
    if (axisEditor->left->isChecked() == left) count++;
  }
  return count;
}

std::optional<int> TimeChartEditWidget::firstAxisOnSide(bool left) const
{
  int const tabCount = optionsEditor->axes->count();
  for (int i = 0; i < tabCount; i++) {
    TimeChartAxisEditor const *axisEditor =
      static_cast<TimeChartAxisEditor const *>(optionsEditor->axes->widget(i));
    if (axisEditor->left->isChecked() == left) return i;
  }
  return std::nullopt;
}

int TimeChartEditWidget::axisCount() const
{
  return optionsEditor->axes->count();
}

std::optional<conf::DashWidgetChart::Axis const>
  TimeChartEditWidget::axis(int num) const
{
  if (num >= optionsEditor->axes->count()) {
    qWarning() << "TimeChartEditWidget: Asked axis number" << num
               << "but have only" << optionsEditor->axes->count();
    return std::nullopt;
  }

  TimeChartAxisEditor const *axisEditor =
    static_cast<TimeChartAxisEditor const *>(optionsEditor->axes->widget(num));
  return axisEditor->getValue();
}

void TimeChartEditWidget::iterFields(std::function<void(
  std::string const &site, std::string const &program, std::string const &function,
  conf::DashWidgetChart::Column const &)> cb) const
{
  int const numFunctions = functionsEditor->functions->count();

  if (verbose)
    qDebug() << "TimeChartEditWidget: iter over" << numFunctions << "functions";

  for (int i = 0; i < numFunctions; i++) {
    TimeChartFunctionEditor const *funcEditor =
      static_cast<TimeChartFunctionEditor const *>(
        functionsEditor->functions->widget(i));

    if (! funcEditor->visible->isChecked()) continue;

    conf::DashWidgetChart::Source const &source =
      funcEditor->model->source;
    for (conf::DashWidgetChart::Column const &field : source.fields) {
      if (field.representation == conf::DashWidgetChart::Column::Unused)
        continue;

      cb(source.site, source.program, source.function, field);
    }
  }
}

void TimeChartEditWidget::closeEvent(QCloseEvent *event)
{
  cancelButton->click();
  AtomicWidget::closeEvent(event);
}
