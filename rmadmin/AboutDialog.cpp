#include <QLabel>
#include <QHBoxLayout>
#include <QVBoxLayout>
#include <QFormLayout>
#include "../src/config.h"
#include "Resources.h"
#include "AboutDialog.h"

AboutDialog::AboutDialog(QWidget *parent) :
  QDialog(parent)
{
  QVBoxLayout *titleLayout = new QVBoxLayout;
  QLabel *title = new QLabel(QString("RmAdmin"));
  titleLayout->addWidget(title);
  QLabel *version = new QLabel(QString("Version %1").arg(PACKAGE_VERSION));
  titleLayout->addWidget(version);

  QHBoxLayout *headLayout = new QHBoxLayout;
  headLayout->addStretch();
  QLabel *icon = new QLabel;
  icon->setPixmap(Resources::get()->applicationIcon);
  headLayout->addWidget(icon);
  headLayout->addLayout(titleLayout);
  headLayout->addStretch();

  QLabel *homePage =
    new QLabel(QString("<a href=\"https://rixed.github.io/ramen\">"
                       "https://rixed.github.io/ramen</a>"));
  homePage->setOpenExternalLinks(true);
  QLabel *contact =
    new QLabel(QString("<a href=\"mailto:rixed@happyleptic.org\">"
                       "rixed@happyleptic.org</a>"));
  contact->setOpenExternalLinks(true);
  QLabel *sourceCode =
    new QLabel(QString("<a href=\"https://github.com/rixed/ramen\">"
                       "https://github.com/rixed/ramen</a>"));
  sourceCode->setOpenExternalLinks(true);
  QLabel *license =
    new QLabel(QString("<a href=\"https://github.com/rixed/ramen/blob/master/LICENSE\">"
                       "AGPLv3 + exceptions</a>"));
  license->setOpenExternalLinks(true);

  QFormLayout *infoLayout = new QFormLayout;
  infoLayout->addRow(tr("Home Page"), homePage);
  infoLayout->addRow(tr("Contact"), contact);
  infoLayout->addRow(tr("Source Code"), sourceCode);
  infoLayout->addRow(tr("License"), license);

  QLabel *copyright = new QLabel(QString("© 2019 Accedian"));
  QHBoxLayout *footerLayout = new QHBoxLayout;
  footerLayout->addStretch();
  footerLayout->addWidget(copyright);
  footerLayout->addStretch();

  QVBoxLayout *dialogLayout = new QVBoxLayout;
  dialogLayout->addLayout(headLayout);
  dialogLayout->addLayout(infoLayout);
  dialogLayout->addLayout(footerLayout);
  setLayout(dialogLayout);

  setWindowTitle(tr("About RmAdmin"));
}
