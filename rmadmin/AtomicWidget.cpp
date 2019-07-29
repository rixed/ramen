#include <QStackedLayout>
#include "AtomicWidget.h"

void AtomicWidget::setEnabled(bool enabled)
{
  if (enabled && !last_enabled) {
    // Capture the value at the beginning of edition:
    initValue = getValue();
  }
  last_enabled = enabled;
}

void AtomicWidget::setCentralWidget(QWidget *w)
{
  QStackedLayout *layout = new QStackedLayout;
  layout->addWidget(w);
  setLayout(layout);
}
