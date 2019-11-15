#include <QButtonGroup>
#include <QComboBox>
#include <QCoreApplication>
#include <QDialogButtonBox>
#include <QFileDialog>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QPushButton>
#include <QLabel>
#include <QLineEdit>
#include <QRadioButton>
#include <QSettings>
#include <QStackedLayout>
#include <QStringList>
#include <QVBoxLayout>
#include "LoginWidget.h"

static char const settingGroupName[] = "LoginForm";
static char const recentServersSetting[] = "Servers";
static char const recentUsernamesSetting[] = "Usernames";
static char const recentSecureSetting[] = "LastSecure";
static char const recentIdFilesSettings[] = "IdentityFiles";

LoginWidget::LoginWidget(
  QString const configDir_,
  QWidget *parent) :
    QWidget(parent),
    configDir(configDir_)
{
  QSettings settings;
  settings.beginGroup(settingGroupName);

  /* Goes with the specialized sizeHint: */
  setSizePolicy(QSizePolicy::Minimum, QSizePolicy::Preferred);

  { /* The server url: an editable combobox with the most recently used
       entries */
    serverCombo = new QComboBox;
    serverCombo->setEditable(true);
    serverCombo->setSizePolicy(QSizePolicy::MinimumExpanding, QSizePolicy::Fixed);
    serverCombo->lineEdit()->setPlaceholderText(tr("the server address"));

    QStringList recentServers =
      settings.value(recentServersSetting).toString().split(',');
    recentServers.removeDuplicates();
    serverCombo->addItems(recentServers);
    QString const defaultServer("localhost:29340");
    if (! recentServers.contains(defaultServer))
      serverCombo->addItem(defaultServer);
  }

  { /* A radio button switch between insecure (with username only) and
       secure (with an identity file featured username and keys) */
    bool wasSecure =
      settings.value(recentSecureSetting, false).toBool();

    secureCheck = new QRadioButton(tr("Secure"));
    secureCheck->setChecked(wasSecure);

    insecureCheck = new QRadioButton(tr("Insecure"));
    insecureCheck->setChecked(! wasSecure);

    QButtonGroup *group = new QButtonGroup(this);
    group->addButton(secureCheck);
    group->addButton(insecureCheck);
  }


  { /* The username widget: an editable combo with the max 5 most recently
       used entries and then the actual unix username. */
    usernameCombo = new QComboBox;
    usernameCombo->setEditable(true);
    usernameCombo->setSizePolicy(QSizePolicy::MinimumExpanding, QSizePolicy::Fixed);
    usernameCombo->lineEdit()->setPlaceholderText(tr("your user name"));

    QStringList recentNames =
      settings.value(recentUsernamesSetting).toString().split(',');
    recentNames.removeDuplicates();
    usernameCombo->addItems(recentNames);
    QString systemName = QString::fromLocal8Bit(qgetenv("USER"));
    if (systemName.isEmpty())
      systemName = QString::fromLocal8Bit(qgetenv("USERNAME"));
    if (! systemName.isEmpty() && ! recentNames.contains(systemName))
      usernameCombo->addItem(systemName);
  }

  { /* The identity file is chosen from a non editable combobox containing
       the recently used files, with a file picker to set the new top value: */
    idFileCombo = new QComboBox;
    idFileCombo->setEditable(true);
    idFileCombo->setSizePolicy(QSizePolicy::MinimumExpanding, QSizePolicy::Fixed);
    idFileCombo->setSizeAdjustPolicy(QComboBox::AdjustToContents);
    idFileCombo->setInsertPolicy(QComboBox::NoInsert);
    QStringList recentFiles =
      settings.value(recentIdFilesSettings).toString().split(',');
    recentFiles.removeDuplicates();
    idFileCombo->addItems(recentFiles);
    pickFileButton = new QPushButton(tr("or select…"));
  }

  { /* The buttons */
    submitButton = new QPushButton(tr("Connect"));
    submitButton->setDefault(true);
    connectStatus = new QLabel;
    cancelButton = new QPushButton(tr("Cancel"));
  }

  /* Layout all this: first server, then username, then secure/idfile,
   * then the buttons. So the top part is a formLayout and the bottom part
   * a stacked layout: */
  QVBoxLayout *outerLayout = new QVBoxLayout;

  { // A form layout with the server, username and identity:
    QFormLayout *formLayout = new QFormLayout;
    formLayout->addRow(tr("Server:"), serverCombo);
    /* Tooltip: "Address of the configuration server, as a host name or an IP, "
    "optionally followed by a colon and a port number." */

    formLayout->addRow(insecureCheck, usernameCombo);

    { // the horizontal box with both file combo and picker button
      QHBoxLayout *identityBox = new QHBoxLayout;
      identityBox->addWidget(idFileCombo);
      identityBox->addWidget(pickFileButton);
      formLayout->addRow(secureCheck, identityBox);
    }
    outerLayout->addLayout(formLayout);
  }

  { // A button group and the hidden label in a stacked layout
    buttonsOrStatus = new QStackedLayout;
    { // The button box
      QDialogButtonBox *buttonBox = new QDialogButtonBox;
      buttonBox->addButton(cancelButton, QDialogButtonBox::RejectRole);
      buttonBox->addButton(submitButton, QDialogButtonBox::AcceptRole);
      buttonsIdx = buttonsOrStatus->addWidget(buttonBox);
    }
    { // The status text
      statusIdx = buttonsOrStatus->addWidget(connectStatus);
    }
    outerLayout->addLayout(buttonsOrStatus);
  }

  setLayout(outerLayout);

  // Wire up everything
  connect(secureCheck, &QRadioButton::toggled,
          this, &LoginWidget::setSecure);
  connect(pickFileButton, &QPushButton::clicked,
          this, &LoginWidget::pickNewFile);
  connect(submitButton, &QPushButton::clicked,
          this, &LoginWidget::submitLogin);
  connect(cancelButton, &QPushButton::clicked,
          this, &LoginWidget::cancelled);

  setSecure(secureCheck->isChecked());

  settings.endGroup();
}

void LoginWidget::focusSubmit()
{
  submitButton->setFocus(Qt::OtherFocusReason);
}

void LoginWidget::resizeFileCombo()
{
  QString const text = idFileCombo->lineEdit()->text();
  QFont font("", 0);
  QFontMetrics fm(font);
  idFileCombo->lineEdit()->setFixedWidth(
# if QT_VERSION >= QT_VERSION_CHECK(5, 11, 0)
    fm.horizontalAdvance(text)
# else
    fm.width(text)
# endif
  );
}

void LoginWidget::setSecure(bool isSecure)
{
  usernameCombo->setEnabled(!isSecure);
  idFileCombo->setEnabled(isSecure);
  pickFileButton->setEnabled(isSecure);
}

void LoginWidget::pickNewFile()
{
  QString fileName =
    QFileDialog::getOpenFileName(this, tr("Select an Identity File"),
      configDir, tr("Identity File (*)"));

  if (fileName.isNull()) return;

  idFileCombo->lineEdit()->setText(fileName);
  // Also insert it in the combo to resize it:
  idFileCombo->addItem(fileName);
}

static QString savedComboEntries(QComboBox *combo)
{
  QStringList items;
  if (combo->isEditable()) {
    QString item = combo->lineEdit()->text();
    if (! item.isEmpty()) items += item;
  }
  for (int i = 0; i < combo->count(); i ++) {
    QString item = combo->itemText(i);
    if (! item.isEmpty()) items += item;
  }
  items.removeDuplicates();
  return items.join(',');
}

void LoginWidget::submitLogin()
{
  // Save the form values in the settings without further ado:
  QSettings settings;
  settings.beginGroup(settingGroupName);
  settings.setValue(recentServersSetting, savedComboEntries(serverCombo));
  settings.setValue(recentUsernamesSetting, savedComboEntries(usernameCombo));
  settings.setValue(recentSecureSetting, secureCheck->isChecked());
  settings.setValue(recentIdFilesSettings, savedComboEntries(idFileCombo));
  settings.endGroup();

  // Once the settings are safe, starts the app:
  emit submitted(
    serverCombo->currentText(),
    usernameCombo->currentText(),
    secureCheck->isChecked() ?
      idFileCombo->currentText() : QString());
}

void LoginWidget::setSubmitStatus(QString const text)
{
  /* If its the first time we are called, disable and hide the submit button,
   * and save the used parameters in the settings. */
  if (buttonsOrStatus->currentIndex() == buttonsIdx) {
    serverCombo->setEnabled(false);
    usernameCombo->setEnabled(false);
    insecureCheck->setEnabled(false);
    secureCheck->setEnabled(false);
    idFileCombo->setEnabled(false);
    pickFileButton->setEnabled(false);
    submitButton->setEnabled(false);
    cancelButton->setEnabled(false);
    buttonsOrStatus->setCurrentIndex(statusIdx);
  }
  connectStatus->setText(text);
}
