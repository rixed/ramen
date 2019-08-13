#include "ConfTreeWidget.h"
#include "ConfTreeDialog.h"

ConfTreeDialog::ConfTreeDialog(QWidget *parent) :
  SavedWindow("ConfigTreeWindow", tr("Raw Configuration"), parent)
{
  confTreeWidget = new ConfTreeWidget(this);
  setCentralWidget(confTreeWidget);
}
