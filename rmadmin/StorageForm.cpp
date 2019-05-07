#include <QFormLayout>
#include <QWidget>
#include "PosIntValidator.h"
#include "PosDoubleValidator.h"
#include "StorageForm.h"
#include "KLineEdit.h"

StorageForm::StorageForm(QWidget *parent) :
  AtomicForm(tr("Storage"), parent)
{
  QWidget *cw = new QWidget(this);

  /* Define 2 input lines, for total size and recall cost: */

  static std::string const totalSizeKey("storage/total_size");
  KLineEdit *totalSizeEdit = new KLineEdit(totalSizeKey, conf::ValueType::IntType, cw);
  totalSizeEdit->setPlaceholderText(tr("Size in bytes"));
  totalSizeEdit->setValidator(&posIntValidator);

  static std::string const recallCostKey("storage/recall_cost");
  KLineEdit *recallCostEdit = new KLineEdit(recallCostKey, conf::ValueType::FloatType, cw);
  recallCostEdit->setPlaceholderText(tr("Cost of reading vs. computing"));
  recallCostEdit->setValidator(&posDoubleValidator);

  QFormLayout *formLayout = new QFormLayout;
  formLayout->addRow(tr("&Total Size:"), totalSizeEdit);
  formLayout->addRow(tr("&Recall Cost:"), recallCostEdit);
  cw->setLayout(formLayout);

  this->setCentralWidget(cw);
  this->addWidget(totalSizeEdit);
  this->addWidget(recallCostEdit);
}

StorageForm::~StorageForm()
{
}
