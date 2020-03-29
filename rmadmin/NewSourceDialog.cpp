#include <ctime>
#include <QComboBox>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLineEdit>
#include <QVBoxLayout>
#include "CodeEdit.h"
#include "conf.h"
#include "confValue.h"
#include "KTextEdit.h"
#include "PathNameValidator.h"
#include "RamenValue.h"

#include "NewSourceDialog.h"

NewSourceDialog::NewSourceDialog(QWidget *parent) :
  QDialog(parent)
{
  nameEdit = new QLineEdit;
  nameEdit->setPlaceholderText("Unique name");
  nameEdit->setValidator(new PathNameValidator(this));
  // TODO: Validate that the name is unique

  codeEdit = new CodeEdit;
  codeEdit->setEnabled(true);

  QDialogButtonBox *buttonBox =
    new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);

  connect(buttonBox, &QDialogButtonBox::accepted,
          this, &NewSourceDialog::createSource);
  connect(buttonBox, &QDialogButtonBox::rejected,
          this, &QDialog::reject);

  QFormLayout *formLayout = new QFormLayout;
  formLayout->addRow(tr("Source name"), nameEdit);
  formLayout->addRow(codeEdit);
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addLayout(formLayout);
  layout->addWidget(buttonBox);
  setLayout(layout);

  setWindowTitle(tr("Create New Source"));
  setModal(true);
}

void NewSourceDialog::createSource()
{
  QString const extension =
    codeEdit->extensionsCombo->currentData().toString();
  std::shared_ptr<conf::Value const> val = codeEdit->getValue();

  std::string key("sources/" + nameEdit->text().toStdString() +
                "/" + extension.toStdString());
  askNew(key, val);

  clear();
  emit QDialog::accept();
}

void NewSourceDialog::clear()
{
  codeEdit->setLanguage(codeEdit->textEditorIndex);
  codeEdit->enableLanguage(codeEdit->infoEditorIndex, false);
  codeEdit->textEditor->setValue("",
    std::make_shared<conf::RamenValueValue>(new VString(
      QString("-- Created by ") + *my_uid +
      QString(" the ") + stringOfDate(std::time(nullptr)))));
}
