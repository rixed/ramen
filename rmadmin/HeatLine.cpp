#include <cassert>
#include <QDebug>
#include <QPainter>
#include <QPaintEvent>
#include <QRect>
#include "HeatLine.h"

HeatLine::HeatLine(
    qreal beginOftime, qreal endOfTime,
    bool withCursor,
    bool doScroll,
    QWidget *parent)
  : AbstractTimeLine(beginOftime, endOfTime, withCursor, doScroll, parent)
{
  blocks.reserve(20);
}

void HeatLine::paintEvent(QPaintEvent *event)
{
  QPainter painter(this);
  painter.setPen(Qt::NoPen);

  // TODO: sort the blocks if not sorted already

  static QColor const hoveredColor("dimgrey");
  if (hovered) {
    if (blocks.empty()) {
      painter.fillRect(event->rect(), hoveredColor);
    } else {
      int const xStart(toPixel(blocks[0].first));
      painter.setBrush(hoveredColor);
      painter.drawRect(0, 0, xStart, height());
    }
  }

  for (int i = 0; i < blocks.size(); i ++) {
    QPair<qreal, std::optional<QColor>> const &block = blocks[i];

    if (! block.second && ! hovered) continue;

    bool const isLast = i == blocks.size() - 1;
    qreal const start(block.first);
    qreal const end(isLast ? m_endOfTime : blocks[i+1].first);

    int const xStart(toPixel(start));

    if (block.second) {
      painter.setBrush(hovered ?
        block.second->lighter() :
        *block.second);
    } else {
      assert(hovered);
      painter.setBrush(hoveredColor);
    }
    painter.drawRect(xStart, 0, toPixel(end) - xStart, height());
  }

  // Paint the cursor over the heatmap:
  AbstractTimeLine::paintEvent(event);
}

void HeatLine::add(qreal start, std::optional<QColor> const &color)
{
  blocks.append(QPair<qreal, std::optional<QColor>>(start, color));

  if (start < m_beginOfTime) {
    setBeginOfTime(start);
    emit beginOfTimeChanged(start);
  }
  if (start > m_endOfTime) {
    setEndOfTime(start);
    emit endOfTimeChanged(start);
  }
}
