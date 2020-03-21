#include <QDialogButtonBox>
#include <QFormLayout>
#include <QLineEdit>
#include <QVBoxLayout>
#include "conf.h"
#include "confValue.h"
#include "PathNameValidator.h"

#include "dashboard/NewDashboardDialog.h"

NewDashboardDialog::NewDashboardDialog(QWidget *parent)
  : QDialog(parent)
{
  nameEdit = new QLineEdit;
  nameEdit->setPlaceholderText("Unique name");
  nameEdit->setValidator(new PathNameValidator(this));
  // TODO: Validate that the name is unique

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
}

void NewDashboardDialog::createDashboard()
{
  /* Originally the dashboard is created with a single text widget with
   * the name, as a placeholder, since empty dashboards are invalid (not
   * stored in the configuration). */
  QString const &name = nameEdit->text();
  std::shared_ptr<conf::Value const> val =
    std::make_shared<conf::DashboardWidgetText const>(name);
  std::string key("dashboards/" + nameEdit->text().toStdString() +
                  "/widgets/0");
  askNew(key, val);

  clear();
  emit QDialog::accept();
}

void NewDashboardDialog::clear()
{
  nameEdit->clear();
}
