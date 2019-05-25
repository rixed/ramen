#include <QHBoxLayout>
#include <QPushButton>
#include <QComboBox>
#include "TailTableBar.h"

TailTableBar::TailTableBar(QWidget *parent) :
  QWidget(parent)
{
  QHBoxLayout *layout = new QHBoxLayout;

  quickChartButton = new QPushButton(tr("Plot the selected columns"));
  layout->addWidget(quickChartButton);

  addToCombo = new QComboBox;
  layout->addWidget(addToCombo);

  setLayout(layout);
}

void TailTableBar::setEnabled(bool enabled)
{
  quickChartButton->setEnabled(enabled);
  addToCombo->setEnabled(enabled);
}
