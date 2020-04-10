#include <QButtonGroup>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QPushButton>
#include <QRadioButton>
#include <QVBoxLayout>
#include "dashboard/DashboardSelector.h"
#include "dashboard/DashboardTreeModel.h"

#include "dashboard/DashboardCopyDialog.h"

DashboardCopyDialog::DashboardCopyDialog(QWidget *parent)
  : QDialog(parent)
{
  setModal(true);

  copyButton = new QRadioButton(tr("copy"));
  moveButton = new QRadioButton(tr("move"));
  QButtonGroup *copyMoveGroup = new QButtonGroup(this);
  copyMoveGroup->addButton(copyButton);
  copyMoveGroup->addButton(moveButton);
  copyButton->setChecked(true);

  QHBoxLayout *copyMoveLayout = new QHBoxLayout;
  copyMoveLayout->addWidget(copyButton);
  copyMoveLayout->addWidget(moveButton);

  dashSelector =
    new DashboardSelector(DashboardTreeModel::globalDashboardTree);

  QPushButton *acceptButton = new QPushButton(tr("Ok"));
  connect(acceptButton, &QPushButton::clicked,
          this, &DashboardCopyDialog::accept);

  QPushButton *cancelButton = new QPushButton(tr("Cancel"));
  connect(cancelButton, &QPushButton::clicked,
          this, &DashboardCopyDialog::reject);

  QFormLayout *form = new QFormLayout;
  form->addRow(tr("Action:"), copyMoveLayout);
  form->addRow(tr("Destination:"), dashSelector);

  QHBoxLayout *buttonBox = new QHBoxLayout;
  buttonBox->addStretch();
  buttonBox->addWidget(cancelButton);
  buttonBox->addWidget(acceptButton);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addLayout(form);
  layout->addLayout(buttonBox);
  setLayout(layout);
}

int DashboardCopyDialog::copy(bool defaultToCopy)
{
  // Surprisingly, copyButton->setChecked(defaultToCopy) does not do the job
  if (defaultToCopy)
    copyButton->setChecked(true);
  else
    moveButton->setChecked(true);

  return QDialog::exec();
}
