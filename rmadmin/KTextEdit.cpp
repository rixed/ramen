#include <iostream>
#include <cassert>
#include <QFontMetrics>
#include "once.h"
#include "RamenSyntaxHighlighter.h"
#include "KTextEdit.h"

KTextEdit::KTextEdit(conf::Key const &key, QWidget *parent) :
  AtomicWidget(key, parent)
{
  textEdit = new QTextEdit;
  setCentralWidget(textEdit);
  new RamenSyntaxHighlighter(textEdit->document()); // the document becomes owner
  textEdit->setFontFamily("monospace");

  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  if (kv.isSet()) {
    bool ok = setValue(key, kv.val);
    assert(ok); // ?
  }
  setEnabled(kv.isMine());
  conf::kvs_lock.unlock_shared();

  Once::connect(&kv, &KValue::valueCreated, this, &KTextEdit::setValue);
  connect(&kv, &KValue::valueChanged, this, &KTextEdit::setValue);
  connect(&kv, &KValue::valueLocked, this, &KTextEdit::lockValue);
  connect(&kv, &KValue::valueUnlocked, this, &KTextEdit::unlockValue);
  // TODO: valueDeleted.
}

std::shared_ptr<conf::Value const> KTextEdit::getValue() const
{
  return std::shared_ptr<conf::Value const>(
    new conf::RamenValueValue(new VString(textEdit->toPlainText())));
}

void KTextEdit::setEnabled(bool enabled)
{
  AtomicWidget::setEnabled(enabled);
  textEdit->setReadOnly(! enabled);
}

bool KTextEdit::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString(k));
  if (new_v != textEdit->toPlainText()) {
    textEdit->setPlainText(new_v);

    /* We'd like the size to be that of the widest line of text: */
    QFont font(textEdit->document()->defaultFont());
    QFontMetrics fontMetrics((QFontMetrics(font)));
    int const tabWidth = 20;  // FIXME: get this from somewhere
    int const maxHeight = 500;
    QSize textSize(fontMetrics.size(Qt::TextExpandTabs, new_v, tabWidth));
    textEdit->setMinimumSize(
      textSize.width(), std::min(maxHeight, textSize.height()));

    emit valueChanged(k, v);
  }

  return true;
}
