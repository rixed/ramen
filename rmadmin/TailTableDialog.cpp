#include "FunctionItem.h"
#include "TailModel.h"
#include "TailTable.h"
#include "TailTableDialog.h"

TailTableDialog::TailTableDialog(
  std::shared_ptr<Function> function_, QWidget *parent) :
  QMainWindow(parent),
  function(function_)
{
  setUnifiedTitleAndToolBarOnMac(true);

  /* Such a window will be created each time a new function tail is
   * asked. So we want them to be deleted for real when closed (no
   * merely hidden): */
  setAttribute(Qt::WA_DeleteOnClose);

  TailTable *table = new TailTable(function);

  setCentralWidget(table);

  setWindowTitle(tr("Tail of %1").arg(function->fqName));
}
