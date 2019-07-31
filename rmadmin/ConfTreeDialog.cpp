#include <QStackedLayout>
#include "ConfTreeWidget.h"
#include "ConfTreeDialog.h"

ConfTreeDialog::ConfTreeDialog(QWidget *parent) :
  QDialog(parent)
{
  confTreeWidget = new ConfTreeWidget(this);

  QStackedLayout *layout = new QStackedLayout;
  // TODO: Add a search box up as the first line
  layout->addWidget(confTreeWidget);
  layout->setContentsMargins(QMargins());
  setLayout(layout);

  setWindowTitle(tr("Raw Configuration"));
  setSizeGripEnabled(true); // editors of various types vary largely in size
}
