#include "ConfTreeWidget.h"
#include "ConfTreeDialog.h"

ConfTreeDialog::ConfTreeDialog(QWidget *parent) :
  SavedWindow("ConfigTreeWindow", tr("Raw Configuration"), true, parent)
{
  confTreeWidget = new ConfTreeWidget(this);
  setCentralWidget(confTreeWidget);
}
