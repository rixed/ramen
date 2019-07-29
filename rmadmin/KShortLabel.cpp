#include <QPainter>
#include <QFontMetrics>
#include <QTextLayout>
#include "KShortLabel.h"

KShortLabel::KShortLabel(conf::Key const &key, QWidget *parent) :
  AtomicWidget(key, parent)
{
  frame = new QFrame;
  setCentralWidget(frame);
  conf::kvs_lock.lock_shared();
  KValue &kv = conf::kvs[key];
  if (kv.isSet()) {
    bool ok = setValue(key, kv.val);
    assert(ok); // ?
  }
  setEnabled(kv.isMine());
  conf::kvs_lock.unlock_shared();

  connect(&kv, &KValue::valueCreated, this, &KShortLabel::setValue);
  connect(&kv, &KValue::valueChanged, this, &KShortLabel::setValue);

  setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Preferred);
}

bool KShortLabel::setValue(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString new_v(v->toQString());

  if (new_v != text) {
    text = new_v;
    update();
    emit valueChanged(k, v);
  }

  return true;
}

void KShortLabel::paintEvent(QPaintEvent *event)
{
  QPainter painter(this);
  QFontMetrics fontMetrics = painter.fontMetrics();
  QRect const bbox = event->rect();

  int lineSpacing = fontMetrics.lineSpacing();
  int y = 0;
  QStringList lines(text.split('\n'));

  for (QString const l : lines) {

    if (y >= bbox.bottom() || y >= height()) break;

    if (y <= bbox.bottom()) {
      QTextLayout textLayout(l, painter.font());
      textLayout.beginLayout();
      QTextLine line = textLayout.createLine();
      if (! line.isValid()) continue; // ?
      line.setLineWidth(width());

      QString elidedLastLine =
        fontMetrics.elidedText(l, Qt::ElideRight, width());
      painter.drawText(QPoint(0, y + fontMetrics.ascent()), elidedLastLine);

      textLayout.endLayout();
    }

    y += lineSpacing;
  }
}
