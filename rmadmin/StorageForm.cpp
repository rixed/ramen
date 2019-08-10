#include <QFormLayout>
#include <QWidget>
#include "PosIntValidator.h"
#include "PosDoubleValidator.h"
#include "StorageForm.h"
#include "KFloatEditor.h"
#include "KIntEditor.h"

StorageForm::StorageForm(QWidget *parent) :
  AtomicForm(parent)
{
  QWidget *cw = new QWidget(this);

  /* Define 2 input lines, for total size and recall cost: */

  static std::string const totalSizeKey("storage/total_size");
  KIntEditor *totalSizeEdit =
    new KIntEditor(&VU64::ofQString, cw);
  totalSizeEdit->setKey(totalSizeKey);
  totalSizeEdit->setPlaceholderText(tr("Size in bytes"));

  static std::string const recallCostKey("storage/recall_cost");
  KFloatEditor *recallCostEdit = new KFloatEditor(cw, 0., 1.);
  recallCostEdit->setKey(recallCostKey);
  recallCostEdit->setPlaceholderText(tr("Cost of reading vs. computing"));

  QFormLayout *formLayout = new QFormLayout;
  formLayout->addRow(tr("&Total Size:"), totalSizeEdit);
  formLayout->addRow(tr("&Recall Cost:"), recallCostEdit);
  cw->setLayout(formLayout);

  setCentralWidget(cw);
  addWidget(totalSizeEdit);
  addWidget(recallCostEdit);
}
