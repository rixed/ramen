#include <ctime>
#include <QComboBox>
#include <QDebug>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLineEdit>
#include <QPushButton>
#include <QVBoxLayout>
#include "CodeEdit.h"
#include "conf.h"
#include "confValue.h"
#include "KTextEdit.h"
#include "PathNameValidator.h"
#include "RamenValue.h"

#include "NewSourceDialog.h"

static bool const verbose(false);

NewSourceDialog::NewSourceDialog(QWidget *parent) :
  QDialog(parent)
{
  nameEdit = new QLineEdit;
  nameEdit->setPlaceholderText("Unique name");
  nameEdit->setValidator(new PathNameValidator(this));
  connect(nameEdit, &QLineEdit::textChanged,
          this, &NewSourceDialog::checkValidity);
  // TODO: Validate that the name is unique

  codeEdit = new CodeEdit;
  codeEdit->setEnabled(true);
  connect(codeEdit, &CodeEdit::inputChanged,
          this, &NewSourceDialog::checkValidity);

  buttonBox =
    new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);
  buttonBox->button(QDialogButtonBox::Ok)->setEnabled(false);

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

void NewSourceDialog::checkValidity()
{
  if (verbose)
    qDebug() << "NewSourceDialog::checkValidity()";

  buttonBox->button(QDialogButtonBox::Ok)->setEnabled(
    nameEdit->hasAcceptableInput() &&
    codeEdit->hasValidInput());
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
  codeEdit->enableLanguage(codeEdit->alertEditorIndex, true);
  codeEdit->enableLanguage(codeEdit->textEditorIndex, true);
  codeEdit->enableLanguage(codeEdit->infoEditorIndex, false);
  codeEdit->setLanguage(codeEdit->textEditorIndex);
  codeEdit->textEditor->setValue("",
    std::make_shared<conf::RamenValueValue>(new VString(
      QString("-- Created by ") + *my_uid +
      QString(" the ") + stringOfDate(std::time(nullptr)))));
}
