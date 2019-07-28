#include <QRadioButton>
#include <QVBoxLayout>
#include "KBool.h"

KBool::KBool(conf::Key const &key, QString const &yesLabel, QString const &noLabel, QWidget *parent) :
  KChoice(
    key,
    {
      { yesLabel, std::shared_ptr<conf::Value const>(new conf::RamenValueValue(new VBool(true))) },
      { noLabel, std::shared_ptr<conf::Value const>(new conf::RamenValueValue(new VBool(false))) }
    },
    parent
  ) {}

KBool::KBool(conf::Key const &key, QWidget *parent) :
  KBool(key, tr("yes"), tr("no"), parent) {}
