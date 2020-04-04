#include <QtGlobal>
#include <QDebug>
#include <QFormLayout>
#include <QLabel>
#include "misc.h"
#include "conf.h"
#include "confValue.h"
#include "ServerInfoWidget.h"

static bool const verbose(false);

ServerInfoWidget::ServerInfoWidget(QString const &srvUrl, QWidget *parent) :
  QWidget(parent)
{
  layout = new QFormLayout;

  layout->addRow(new QLabel(tr("Configuration Server:")),
                 new QLabel(srvUrl));

  setLayout(layout);

  connect(kvs, &KVStore::keyChanged,
          this, &ServerInfoWidget::onChange);
}

void ServerInfoWidget::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
        setKey(change.key, change.kv);
        break;
      case KeyChanged:
        setKey(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

void ServerInfoWidget::setKey(std::string const &key, KValue const &kv)
{
  if (key == "time") {
    setLabel(key, kv.val);
  } else {
    static std::string const key_prefix = "versions/";
    if (! startsWith(key, key_prefix)) return;
    setLabel(key.substr(key_prefix.length()), kv.val);
  }
}

void ServerInfoWidget::setLabel(
  std::string const &label_, std::shared_ptr<conf::Value const> value_)
{
  QString const label = QString::fromStdString(label_) + ":";
  QString const value = value_->toQString(label_);

  for (int row = 0; row < layout->rowCount(); row ++) {
    QLabel const *l =
      dynamic_cast<QLabel const *>(
        layout->itemAt(row, QFormLayout::LabelRole)->widget());
    if (l) {
      if (l->text() == label) {
        // Update the value:
        QLabel *v =
          dynamic_cast<QLabel *>(
            layout->itemAt(row, QFormLayout::FieldRole)->widget());
        if (v) {
          v->setText(value);
        } else {
          qCritical() << "ServerInfoWidget has a value that's not a label!?";
        }
        return;
      }
    } else {
      qCritical() << "ServerInfoWidget has a label that's not a label!?";
    }
  }

  // Create a new label:
  if (verbose)
    qDebug() << "ServerInfoWidget:: Creating new server info label for"
             << QString::fromStdString(label_);

  layout->addRow(new QLabel(label),
                 new QLabel(value));
}
