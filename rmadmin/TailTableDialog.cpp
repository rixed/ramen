#include "FunctionItem.h"
#include "TailModel.h"
#include "TailTable.h"
#include "TailTableDialog.h"

TailTableDialog::TailTableDialog(FunctionItem *function_, QWidget *parent) :
  QMainWindow(parent),
  function(function_)
{
  setUnifiedTitleAndToolBarOnMac(true);

  /* Such a window will be created each time a new function tail is
   * asked. So we want them to be deleted for real when closed (no
   * merely hidden): */
  setAttribute(Qt::WA_DeleteOnClose);

  if (! function->tailModel) function->tailModel = new TailModel(function);
  function->tailModel->setUsed(true);

  TailTable *table = new TailTable(function->tailModel);

/*  // Add a Plot when the user ask for it:
  connect(table, &TailTable::quickPlotClicked, this, [this, function](QList<int> const &selectedColumns) {
    this->addQuickPlot(function, selectedColumns);
  });*/

  setCentralWidget(table);

  setWindowTitle(tr("Tail of %1").arg(function->fqName()));
}
