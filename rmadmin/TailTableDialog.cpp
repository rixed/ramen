#include "FunctionItem.h"
#include "TailModel.h"
#include "TailTable.h"
#include "TailTableDialog.h"

TailTableDialog::TailTableDialog(
  std::shared_ptr<TailModel> tailModel,
  QWidget *parent) :
  QMainWindow(parent)
{
  setUnifiedTitleAndToolBarOnMac(true);

  /* Such a window will be created each time a new function tail is
   * asked. So we want them to be deleted for real when closed (no
   * merely hidden): */
  setAttribute(Qt::WA_DeleteOnClose);

  TailTable *table = new TailTable(tailModel);

  setCentralWidget(table);

  setWindowTitle(tr("Tail of %1").arg(tailModel->fqName));
}
