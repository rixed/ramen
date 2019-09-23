#include <iostream>
#include <QFormLayout>
#include <QLabel>
#include "misc.h"
#include "conf.h"
#include "confValue.h"
#include "ServerInfoWidget.h"

static bool verbose = true;

ServerInfoWidget::ServerInfoWidget(QString const &srvUrl, QWidget *parent) :
  QWidget(parent)
{
  layout = new QFormLayout;

  layout->addRow(new QLabel(tr("Configuration Server:")),
                 new QLabel(srvUrl));

  setLayout(layout);

  connect(&kvs, &KVStore::valueCreated,
          this, &ServerInfoWidget::setKey);
}

void ServerInfoWidget::setKey(KVPair const &kvp)
{
  static std::string const key_prefix = "versions/";
  if (! startsWith(kvp.first, key_prefix)) return;

  QString const what =
    QString::fromStdString(kvp.first.substr(key_prefix.length()));

  for (int row = 0; row < layout->rowCount(); row ++) {
    QLabel const *label =
      dynamic_cast<QLabel const *>(
        layout->itemAt(row, QFormLayout::LabelRole)->widget());
    if (label) {
      if (label->text() == what) return;
    } else {
      std::cerr << "ServerInfoWidget has a label that's not a label!?" << std::endl;
    }
  }

  if (verbose)
    std::cout << "Creating new server info label for " << kvp.first << std::endl;
  layout->addRow(new QLabel(what + ":"),
                 new QLabel(kvp.second.val->toQString(kvp.first)));
}
