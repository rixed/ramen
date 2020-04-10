#include <QApplication>
#include <QDebug>
#include <QMouseEvent>
#include <QPainter>

#include "IconSelector.h"

int const IconSelector::iconMargin(2);

IconSelector::IconSelector(QList<QPixmap>pixmaps_, QWidget *parent)
  : QWidget(parent), pixmaps(pixmaps_),
    minSize(iconMargin, iconMargin)
{
  setMouseTracking(true);

  for (int i = 0; i < pixmaps.count(); i++) {
    minSize.setWidth(minSize.width() + pixmaps[i].width() + iconMargin);
    minSize.setHeight(
      std::max(minSize.height(), pixmaps[i].height() + 2*iconMargin));
  }
}

void IconSelector::paint(
  QPainter &painter,
  QRect const &rect,
  QList<QPixmap> const &pixmaps,
  std::optional<int> selected,
  std::optional<int> hovered)
{
  painter.save();
  painter.fillRect(rect, painter.background());
  QPalette const palette(QApplication::palette());
  painter.setPen(palette.color(QPalette::Highlight));

  int x(rect.x());
  int const y(rect.y());
  for (int i = 0; i < pixmaps.count(); i++) {
    if (selected == i) {
      painter.drawRect(
        x, y,
        iconMargin + pixmaps[i].width(),
        iconMargin + pixmaps[i].height());
    }
    if (hovered && *hovered == i) {
      QBrush brush(painter.background());
      brush.setColor(brush.color().lighter());
      QRect bg(pixmaps[i].rect());
      bg.translate(x + iconMargin, iconMargin);
      painter.fillRect(bg, brush);
    }
    painter.drawPixmap(x + iconMargin, y + iconMargin, pixmaps[i]);
    x += iconMargin + pixmaps[i].width();
  }
  painter.restore();
}

void IconSelector::paintEvent(QPaintEvent *)
{
  QPainter painter(this);
  paint(painter, rect(), pixmaps, m_selected, hovered);
}

std::optional<int> IconSelector::iconAtPos(QPoint const &pos)
{
  if (
    pos.y() < iconMargin || pos.y() > minSize.height() - iconMargin ||
    pos.x() < iconMargin || pos.x() > minSize.width() - iconMargin
  ) return std::nullopt;

  int x(iconMargin);
  for (int i = 0; i < pixmaps.count(); i++) {
    if (pos.x() < x + pixmaps[i].width()) {
      return i;
    }
    x += pixmaps[i].width() + iconMargin;
  }

  return std::nullopt;
}

void IconSelector::mouseMoveEvent(QMouseEvent *event)
{
  std::optional<int> const prev(hovered);
  hovered = iconAtPos(event->pos());

  if (hovered != prev)
    update();
}

void IconSelector::mousePressEvent(QMouseEvent *event)
{
  if (event->button() != Qt::LeftButton) return;
  clicked = iconAtPos(event->pos());
}

void IconSelector::mouseReleaseEvent(QMouseEvent *event)
{
  if (! clicked) return;
  if (clicked != iconAtPos(event->pos())) return;

  m_selected = *clicked;
  emit selectionChanged(m_selected);
}
