#include <QHBoxLayout>
#include <QPushButton>
#include <QComboBox>
#include "TailTableBar.h"

TailTableBar::TailTableBar(QWidget *parent) :
  QWidget(parent)
{
  QHBoxLayout *layout = new QHBoxLayout;

  quickPlotButton = new QPushButton(tr("Plot the selected columns"));
  layout->addWidget(quickPlotButton);

  addToCombo = new QComboBox;
  layout->addWidget(addToCombo);

  setLayout(layout);

  connect(quickPlotButton, &QPushButton::clicked,
          this, &TailTableBar::quickPlotClicked);
}

void TailTableBar::setEnabled(bool enabled)
{
  quickPlotButton->setEnabled(enabled);
  addToCombo->setEnabled(enabled);
}
