#include <QRadioButton>
#include <QVBoxLayout>
#include "confValue.h"
#include "KBool.h"

KBool::KBool(QString const &yesLabel, QString const &noLabel, QWidget *parent) :
  KChoice(
    {
      { yesLabel, std::shared_ptr<conf::Value const>(new conf::RamenValueValue(new VBool(true))) },
      { noLabel, std::shared_ptr<conf::Value const>(new conf::RamenValueValue(new VBool(false))) }
    },
    parent
  ) {}

KBool::KBool(QWidget *parent) :
  KBool(tr("yes"), tr("no"), parent) {}
