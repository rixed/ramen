#include <iostream>
#include <math.h>
#include <cassert>
#include <QFontMetrics>
#include <QPlainTextEdit>
#include "once.h"
#include "RamenSyntaxHighlighter.h"
#include "KValue.h"
#include "KTextEdit.h"

KTextEdit::KTextEdit(QWidget *parent) :
  AtomicWidget(parent)
{
  textEdit = new QPlainTextEdit;
  setCentralWidget(textEdit);
  new RamenSyntaxHighlighter(textEdit->document()); // the document becomes owner

  /* Set a monospaced font: */
  QFont font = textEdit->document()->defaultFont();
  font.setFamily("Courier New");
  textEdit->document()->setDefaultFont(font);

  /* Set tab stops to 4 spaces: */
  QFontMetricsF fontMetrics(font);
  float tabWidth = fontMetrics.width("    ");
  textEdit->setTabStopDistance(roundf(tabWidth));

  connect(textEdit, &QPlainTextEdit::textChanged, // ouch!
          this, &KTextEdit::inputChanged);
}

void KTextEdit::extraConnections(KValue *kv)
{
  Once::connect(kv, &KValue::valueCreated, this, &KTextEdit::setValue);
  connect(kv, &KValue::valueChanged, this, &KTextEdit::setValue);
  connect(kv, &KValue::valueLocked, this, &KTextEdit::lockValue);
  connect(kv, &KValue::valueUnlocked, this, &KTextEdit::unlockValue);
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
