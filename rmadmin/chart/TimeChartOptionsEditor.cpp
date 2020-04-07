#include <QDebug>
#include <QLabel>
#include <QLineEdit>
#include <QTabWidget>
#include <QVBoxLayout>
#include "confValue.h"
#include "chart/TimeChartAxisEditor.h"
#include "chart/TimeChartEditWidget.h"

#include "chart/TimeChartOptionsEditor.h"

TimeChartOptionsEditor::TimeChartOptionsEditor(
  TimeChartEditWidget *editWidget_,
  QWidget *parent)
  : QWidget(parent),
    editWidget(editWidget_)
{
  title = new QLineEdit;
  title->setPlaceholderText(tr("Enter a title"));

  axes = new QTabWidget;

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(title);
  layout->addWidget(axes);
  setLayout(layout);

  connect(axes, &QTabWidget::currentChanged,
          this, &TimeChartOptionsEditor::axisChanged);
}

void TimeChartOptionsEditor::setEnabled(bool enabled)
{
  for (int i = 0; i < axes->count(); i++) {
    TimeChartAxisEditor *e =
      static_cast<TimeChartAxisEditor *>(axes->widget(i));
    e->setEnabled(enabled);
  }
}

bool TimeChartOptionsEditor::setValue(
  std::string const &,
  std::shared_ptr<conf::DashWidgetChart const> conf)
{
  title->setText(conf->title);

  int t_idx(0); // iterates over tabs
  size_t c_idx(0); // iterates over conf->axes

  while (t_idx < axes->count() || c_idx < conf->axes.size()) {
    if (t_idx >= axes->count()) {
      TimeChartAxisEditor *e = new TimeChartAxisEditor;
      connect(e, &TimeChartAxisEditor::valueChanged,
              this, [this, t_idx]() {
          emit axisChanged(t_idx);
      });
      // TODO: a name depending on the properties, updated when they change:
      QString name(QString::number(t_idx));
      axes->addTab(e, name);
      e->setValue(conf->axes[c_idx]);
      t_idx++;
      c_idx++;
    } else if (c_idx >= conf->axes.size()) {
      qDebug() << "Removing a tab";
      axes->removeTab(t_idx);
    } else {
      TimeChartAxisEditor *e =
        static_cast<TimeChartAxisEditor *>(axes->widget(t_idx));
      e->setValue(conf->axes[c_idx]);
      t_idx++;
      c_idx++;
    }
  }

  return true;
}

/* Warning: In case signals are reordered, we could have field.axisNum > than
 * axis.count(). Not an issue, just create as many new axis editors as
 * required: */
void TimeChartOptionsEditor::updateAfterFieldChange(
  std::string const &site, std::string const &program,
  std::string const &function, std::string const &name)
{
  editWidget->iterFields([this,&site,&program,&function,&name](
    std::string const &site_, std::string const &program_,
    std::string const &function_, conf::DashWidgetChart::Column const &field) {
    if (site != site_ || program != program_ || function != function_ ||
        name != field.name) return;

    for (int t_idx = axes->count(); t_idx <= field.axisNum; t_idx++) {
      TimeChartAxisEditor *e = new TimeChartAxisEditor;
      connect(e, &TimeChartAxisEditor::valueChanged,
              this, [this, t_idx]() {
          emit axisChanged(t_idx);
      });
      QString name(QString::number(t_idx));
      axes->addTab(e, name);
    }
  });
}
