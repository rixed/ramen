#include <iostream>
#include <ctime>
#include <cassert>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLineEdit>
#include <QComboBox>
#include "conf.h"
#include "confValue.h"
#include "RamenValue.h"
#include "NewSourceDialog.h"

NewSourceDialog::NewSourceDialog(QWidget *parent) :
  QDialog(parent)
{
  nameEdit = new QLineEdit;
  // TODO: Validate that the name is unique
  typeEdit = new QComboBox;
  typeEdit->addItem(tr("Ramen Language"), QString("ramen"));
  typeEdit->addItem(tr("Simple Alert"), QString("alert"));

  QDialogButtonBox *buttonBox =
    new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);

  connect(buttonBox, &QDialogButtonBox::accepted,
          this, &NewSourceDialog::createSource);
  connect(buttonBox, &QDialogButtonBox::rejected,
          this, &QDialog::reject);

  QFormLayout *formLayout = new QFormLayout;
  formLayout->addRow(tr("Name"), nameEdit);
  formLayout->addRow(tr("Type"), typeEdit);
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addLayout(formLayout);
  layout->addWidget(buttonBox);
  setLayout(layout);

  setWindowTitle(tr("Create New Source"));
}

void NewSourceDialog::createSource()
{
  QString const extension = typeEdit->currentData().toString();
  conf::Value *val = nullptr;
  if ("ramen" == extension) {
    val = new conf::RamenValueValue(
      new VString(
        QString("-- Created by ") + *my_uid +
        QString(" the ") + stringOfDate(std::time(nullptr))));
  } else if ("alert" == extension) {
    assert(!"Not implemented");
  } else {
    std::cerr << "Invalid extension: " << extension.toStdString() << std::endl;
    assert(!"Invalid extension");
  }

  conf::Key key("sources/" + nameEdit->text().toStdString() +
                "/" + extension.toStdString());
  conf::askNew(key, std::shared_ptr<conf::Value>(val));

  nameEdit->setText("");
  typeEdit->setCurrentIndex(0);
  emit QDialog::accept();
}
