#include <cassert>
#include <QtGlobal>
#include <QDebug>
#include "conf.h"
#include "confValue.h"
#include "KErrorMsg.h"

KErrorMsg::KErrorMsg(QWidget *parent) :
  QLabel(parent)
{
  connect(kvs, &KVStore::keyChanged,
          this, &KErrorMsg::onChange);
}

void KErrorMsg::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
      case KeyChanged:
        setValueFromStore(change.key, change.kv);
        break;
      case KeyDeleted:
        warnTimeout(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

/* Beware:
 * First, this setKey is not the one from an AtomicWidget.
 * Second, and more importantly, this can (and will) be called before the key
 * is present in kvs (as the string here is taken from the answer to the
 * Auth message)! */
void KErrorMsg::setKey(std::string const &k)
{
  assert(key.length() == 0);
  qDebug() << "KErrorMsg: setting key to" << QString::fromStdString(k);
  key = k;
}

void KErrorMsg::displayError(QString const &str)
{
  QLabel::setStyleSheet(
    str.length() == 0 ?
      QString() :
      QStringLiteral("background-color: pink"));
  QLabel::setText(str);
}

void KErrorMsg::setValueFromStore(std::string const &k, KValue const &kv)
{
  if (key.length() == 0 || key != k) return;

  std::shared_ptr<conf::Error const> err =
    std::dynamic_pointer_cast<conf::Error const>(kv.val);
  if (err) {
    displayError(QString::fromStdString(err->msg));
  } else {
    qCritical() << "Error is not an error, and that's an error!";
    // One wonder how software manage to work sometime
  }
}

void KErrorMsg::warnTimeout(std::string const &k, KValue const &)
{
  if (key.length() == 0 || key != k) return;

  displayError(tr("Server timed us out!"));
}
