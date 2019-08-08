#include "ConfTreeWidget.h"
#include "ConfTreeDialog.h"

ConfTreeDialog::ConfTreeDialog(QWidget *parent) :
  QMainWindow(parent)
{
  setUnifiedTitleAndToolBarOnMac(true);
  confTreeWidget = new ConfTreeWidget(this);

  setCentralWidget(confTreeWidget);

  setWindowTitle(tr("Raw Configuration"));
}
