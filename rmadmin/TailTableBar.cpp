#include <QHBoxLayout>
#include <QPushButton>
#include <QComboBox>
#include "TailTableBar.h"

TailTableBar::TailTableBar(QWidget *parent) :
  QWidget(parent)
{
  QHBoxLayout *layout = new QHBoxLayout;

  QPushButton *quickChartButton = new QPushButton(tr("Plot the selected columns"));
  quickChartButton->setEnabled(false);
  layout->addWidget(quickChartButton);

  QComboBox *addToCombo = new QComboBox;
  addToCombo->setEnabled(false);
  layout->addWidget(addToCombo);

  setLayout(layout);
}
