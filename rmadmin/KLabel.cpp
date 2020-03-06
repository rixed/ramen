#include <QLabel>
#include "confValue.h"
#include "conf.h"

#include "KLabel.h"

KLabel::KLabel(QWidget *parent, bool wordWrap) :
  AtomicWidget(parent)
{
  label = new QLabel;
  label->setWordWrap(wordWrap);
  relayoutWidget(label);
}

bool KLabel::setValue(
  std::string const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString(k));

  if (new_v != label->text()) {
    label->setText(new_v);
    emit valueChanged(k, v);
  }

  return true;
}
