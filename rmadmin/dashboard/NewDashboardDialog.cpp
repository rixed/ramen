#include <QDebug>
#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLineEdit>
#include <QVBoxLayout>
#include "conf.h"
#include "confValue.h"
#include "Menu.h"
#include "PathNameValidator.h"

#include "dashboard/NewDashboardDialog.h"

static bool const verbose { false };

NewDashboardDialog::NewDashboardDialog(QWidget *parent)
  : QDialog(parent)
{
  nameEdit = new QLineEdit;
  nameEdit->setPlaceholderText("Unique name");
  nameEdit->setValidator(new PathNameValidator(this));
  // TODO: Validate that the name is unique and distinct from "scratchpad"

  QDialogButtonBox *buttonBox =
    new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);

  connect(buttonBox, &QDialogButtonBox::accepted,
          this, &NewDashboardDialog::createDashboard);
  connect(buttonBox, &QDialogButtonBox::rejected,
          this, &QDialog::reject);

  QFormLayout *formLayout = new QFormLayout;
  formLayout->addRow(tr("Dashboard name"), nameEdit);
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addLayout(formLayout);
  layout->addWidget(buttonBox);
  setLayout(layout);

  setWindowTitle(tr("Create New Dashboard"));
  setModal(true);
}

void NewDashboardDialog::createDashboard()
{
  if (verbose)
    qDebug() << "NewDashboardDialog: creating new dashboard...";

  /* Originally the dashboard is created with a single text widget with
   * the name, as a placeholder, since empty dashboards are invalid (not
   * stored in the configuration). */
  QString const &name = nameEdit->text();
  assert(name != "scratchpad"); // FIXME

  std::shared_ptr<conf::Value const> val =
    std::make_shared<conf::DashWidgetText const>(tr(
      "Sample text widget where you could enter a description for this widget."));
  std::string const prefix(
    "dashboards/" + nameEdit->text().toStdString());
  std::string key(prefix + "/widgets/0");
  askNew(key, val);

  clear();
  emit QDialog::accept();

  if (verbose)
    qDebug() << "NewDashboardDialog: opening new dashboard...";

  Menu::openDashboard(name, prefix);
}

void NewDashboardDialog::clear()
{
  nameEdit->clear();
}
