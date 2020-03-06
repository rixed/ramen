#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QHBoxLayout>
#include "chart/TimeChartColumnEditor.h"

TimeChartColumnEditor::TimeChartColumnEditor(QWidget *parent)
  : QWidget(parent)
{
  name = new QLabel;
  representation = new QPushButton;
  opacity = new QLineEdit;
  color = new QLineEdit;
  factors = new QLineEdit;
  axis = new QPushButton;

  QHBoxLayout *layout = new QHBoxLayout;
  layout->addWidget(name, 1);
  layout->addWidget(representation);
  layout->addWidget(factors);
  layout->addWidget(axis);
  layout->addWidget(color);
  layout->addWidget(opacity);
}

void TimeChartColumnEditor::setEnabled(bool enabled)
{
  representation->setEnabled(enabled);
  factors->setEnabled(enabled);
  axis->setEnabled(enabled);
  color->setEnabled(enabled);
  opacity->setEnabled(enabled);
}

bool TimeChartColumnEditor::setValue(conf::DashboardWidgetChart::Column const &v)
{
  name->setText(QString::fromStdString(v.column));
  representation->setText(QString::number(static_cast<int>(v.representation)));
  QStringList s;
  for (std::string const &f : v.factors) {
    s += QString::fromStdString(f);
  }
  factors->setText(s.join(','));
  axis->setText(QString::number(v.axisNum));
  color->setText(QString::number(v.color));
  opacity->setText(QString::number(v.opacity));

  return true;
}
