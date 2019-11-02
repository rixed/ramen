#include <cassert>
#include "KLineEdit.h"

KLineEdit::KLineEdit(QWidget *parent) :
  AtomicWidget(parent)
{
  lineEdit = new QLineEdit;
  relayoutWidget(lineEdit);
  connect(lineEdit, &QLineEdit::editingFinished,
          this, &KLineEdit::inputChanged);
}

std::shared_ptr<conf::Value const> KLineEdit::getValue() const
{
  VString *v = new VString(lineEdit->text());
  return std::shared_ptr<conf::Value const>(new conf::RamenValueValue(v));
}

void KLineEdit::setEnabled(bool enabled)
{
  lineEdit->setEnabled(enabled);
}

bool KLineEdit::setValue(std::string const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString(k));

  if (new_v != lineEdit->text()) {
    lineEdit->setText(new_v);
    emit valueChanged(k, v);
  }

  return true;
}
