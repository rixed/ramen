#include <QButtonGroup>
#include <QCheckBox>
#include <QDebug>
#include <QFormLayout>
#include <QRadioButton>
#include <QHBoxLayout>
#include "confValue.h"

#include "TimeChartAxisEditor.h"

TimeChartAxisEditor::TimeChartAxisEditor(QWidget *parent)
  : QWidget(parent)
{
  left = new QRadioButton(tr("Left"));
  right = new QRadioButton(tr("Right"));
  QButtonGroup *side = new QButtonGroup;
  side->addButton(left);
  side->addButton(right);
  left->setChecked(true);

  forceZero = new QCheckBox("Force Zero");

  linear = new QRadioButton(tr("Linear"));
  logarithmic = new QRadioButton(tr("Logarithmic"));
  QButtonGroup *linLog = new QButtonGroup;
  linLog->addButton(linear);
  linLog->addButton(logarithmic);
  linear->setChecked(true);

  connect(side, QOverload<int>::of(&QButtonGroup::buttonClicked),
          this, [this](int){
    emit valueChanged();
  });
  connect(forceZero, &QCheckBox::stateChanged,
          this, &TimeChartAxisEditor::valueChanged);
  connect(linLog, QOverload<int>::of(&QButtonGroup::buttonClicked),
          this, [this](int){
    emit valueChanged();
  });

  QHBoxLayout *sideLayout = new QHBoxLayout;
  sideLayout->addWidget(left);
  sideLayout->addWidget(right);

  QHBoxLayout *scaleLayout = new QHBoxLayout;
  scaleLayout->addWidget(linear);
  scaleLayout->addWidget(logarithmic);

  QFormLayout *form= new QFormLayout;
  form->addRow(tr("Side"), sideLayout);
  form->addRow(tr("Force Zero"), forceZero);
  form->addRow(tr("Scale"), scaleLayout);

  setLayout(form);
}

bool TimeChartAxisEditor::setValue(
  conf::DashWidgetChart::Axis const &a)
{
  if (a.left != left->isChecked()) {
    (a.left ? left : right)->click();
  }

  if (a.forceZero != forceZero->isChecked()) {
    forceZero->setChecked(a.forceZero);
  }

  switch (a.scale) {
    case conf::DashWidgetChart::Axis::Linear:
      if (! linear->isChecked()) {
        linear->click();
      }
      break;

    case conf::DashWidgetChart::Axis::Logarithmic:
      if (! logarithmic->isChecked()) {
        logarithmic->click();
      }
      break;
  }

  return true;
}

conf::DashWidgetChart::Axis TimeChartAxisEditor::getValue() const
{
  return conf::DashWidgetChart::Axis(
    left->isChecked(),
    forceZero->isChecked(),
    linear->isChecked() ?
      conf::DashWidgetChart::Axis::Linear :
      conf::DashWidgetChart::Axis::Logarithmic);
}
