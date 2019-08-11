#include <iostream>
#include <cassert>
#include "once.h"
#include "KLineEdit.h"
#include "PosDoubleValidator.h"
#include "PosIntValidator.h"

KLineEdit::KLineEdit(QWidget *parent) :
  AtomicWidget(parent)
{
  lineEdit = new QLineEdit;
  relayoutWidget(lineEdit);
  connect(lineEdit, &QLineEdit::editingFinished,
          this, &KLineEdit::inputChanged);
}

void KLineEdit::extraConnections(KValue *kv)
{
  Once::connect(kv, &KValue::valueCreated, this, &KLineEdit::setValue);
  connect(kv, &KValue::valueChanged, this, &KLineEdit::setValue);
  connect(kv, &KValue::valueLocked, this, &KLineEdit::lockValue);
  connect(kv, &KValue::valueUnlocked, this, &KLineEdit::unlockValue);
}

std::shared_ptr<conf::Value const> KLineEdit::getValue() const
{
  VString *v = new VString(lineEdit->text());
  return std::shared_ptr<conf::Value const>(new conf::RamenValueValue(v));
}

void KLineEdit::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  lineEdit->setEnabled(enabled);
}

bool KLineEdit::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString(k));
  if (new_v != lineEdit->text()) {
    lineEdit->setText(new_v);
    emit valueChanged(k, v);
  }
  return true;
}
