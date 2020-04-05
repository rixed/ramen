#include <algorithm>
#include <QDebug>
#include <QSize>
#include <QSizePolicy>
#include "GrowingTextEdit.h"

GrowingTextEdit::GrowingTextEdit(QWidget *parent)
  : QTextEdit(parent)
{
  setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Minimum);
  setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
  setVerticalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
  connect(this, &GrowingTextEdit::textChanged,
          this, &GrowingTextEdit::onTextChange);
}

QSize GrowingTextEdit::sizeHint() const
{
  // no size with width:
  document()->setTextWidth(geometry().width());
  QSize s { document()->size().toSize() };
  s.rwidth() = std::max(100, s.width());
  s.rheight() = std::max(20, s.height());
  return s;
}

void GrowingTextEdit::resizeEvent(QResizeEvent *event)
{
  /* Notify layouts that the sizeHint has changed: */
  updateGeometry();
  QTextEdit::resizeEvent(event);
}

void GrowingTextEdit::onTextChange()
{
  QSize s { document()->size().toSize() };
  setMaximumHeight(s.height());
}
