#include <QtGlobal>
#include <QDebug>
#include <QLineEdit>
#include <QLabel>
#include <QPushButton>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include "conf.h"
#include "confValue.h"
#include "PathNameValidator.h"
#include "CloneDialog.h"

static bool const verbose(false);

CloneDialog::CloneDialog(std::string const &origKey, QWidget *parent) :
  QDialog(parent)
{
  QStringList names =
    QString::fromStdString(origKey).split('/', QString::SkipEmptyParts);
  if (names.count() <= 2) {
    qCritical() << "CloneDialog: Invalid origKey:" << QString::fromStdString(origKey);
    newKeyEdit = nullptr;
    cloneButton = nullptr;
    return;
  }

  names.removeFirst();
  extension = names.takeLast();
  origName = names.join('/');

  kvs->lock.lock_shared();
  auto it = kvs->map.find(origKey);
  if (it != kvs->map.end()) value = it->second.val;
  kvs->lock.unlock_shared();

  QVBoxLayout *layout = new QVBoxLayout;

  if (value) {
    QLabel *label = new QLabel(tr("Where to clone %1 into?").arg(origName));
    newKeyEdit = new QLineEdit();
    newKeyEdit->setValidator(new PathNameValidator(this));

    cloneButton = new QPushButton(tr("Clone"));
    cloneButton->setAutoDefault(false);
    cloneButton->setDefault(false);
    cloneButton->setEnabled(false);
    QPushButton *cancelButton = new QPushButton(tr("Cancel"));
    cancelButton->setAutoDefault(false);
    cancelButton->setDefault(false);
    QHBoxLayout *buttonBox = new QHBoxLayout;
    buttonBox->addStretch();
    buttonBox->addWidget(cancelButton);
    buttonBox->addWidget(cloneButton);

    layout->addWidget(label);
    layout->addWidget(newKeyEdit);
    layout->addLayout(buttonBox);

    connect(cloneButton, &QPushButton::clicked,
            this, &CloneDialog::cloneSource);
    connect(cancelButton, &QPushButton::clicked,
            this, &QDialog::reject);
    connect(newKeyEdit, &QLineEdit::textChanged,
            this, &CloneDialog::validate);

  } else {  // no such key

    newKeyEdit = nullptr;
    cloneButton = nullptr;
    QLabel *label =
      new QLabel(tr("Source for %1 (%2) has no value!?")
                   .arg(origName).arg(QString::fromStdString(origKey)));
    layout->addWidget(label);
  }

  setLayout(layout);
}

void CloneDialog::cloneSource()
{
  std::string newKey =
    "sources/" + newKeyEdit->text().toStdString() +
    "/" + extension.toStdString();
  // FIXME: validate that this name is free and valid

  if (verbose)
    qDebug() << "Saving cloned value into" << QString::fromStdString(newKey);

  askNew(newKey, value);
  emit QDialog::accept();
}

void CloneDialog::validate()
{
  bool const isValid = newKeyEdit->hasAcceptableInput();
  cloneButton->setEnabled(isValid);
}
