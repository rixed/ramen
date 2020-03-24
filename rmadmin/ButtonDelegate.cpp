#include <QPixmap>
#include <QPainter>
#include <QMouseEvent>

#include "ButtonDelegate.h"

ButtonDelegate::ButtonDelegate(unsigned margin_, QObject *parent) :
  QStyledItemDelegate(parent),
  margin(margin_)
{}

QRect ButtonDelegate::rect(
  QPixmap const &pixmap, QStyleOptionViewItem const &option) const
{
  // Note: top is toward x = 0. (0, 0) is topleft of the screen.

  // Right-middle aligned. TODO: left, up, down, middle, center, etc...
  QRect r(
    option.rect.right() - margin - pixmap.width(),
    option.rect.center().y() - pixmap.height() / 2.,
    pixmap.width(), pixmap.height());

  int const xMin = option.rect.left() + margin;
  int const yMin = option.rect.top() + margin;
  double scale = 0.;
  if (r.left() < xMin) {
    scale = (option.rect.width() - 2.*margin) / r.width();
  }

  if (r.top() < yMin) {
    double scale2 = (option.rect.height() - 2.*margin) / r.height();
    if (scale2 < scale) scale = scale2;
  }

  if (0 == scale) return r;

  return QRect(
    option.rect.right() - margin - scale * pixmap.width(),
    option.rect.center().y()  - scale * pixmap.height() / 2.,
    scale * pixmap.width(), scale * pixmap.height());
}

void ButtonDelegate::paint(
  QPainter *painter, QStyleOptionViewItem const &option,
  QModelIndex const &index) const
{
  QStyledItemDelegate::paint(painter, option, index);
  if (! (option.state & QStyle::State_MouseOver)) return;

  QVariant data(index.data());
  if (data.canConvert<QPixmap>()) {
    QPixmap pixmap(qvariant_cast<QPixmap>(data));
    painter->drawPixmap(rect(pixmap, option), pixmap);
  }
}

QSize ButtonDelegate::sizeHint(
  QStyleOptionViewItem const &option,
  QModelIndex const &index) const
{
  QSize size(QStyledItemDelegate::sizeHint(option, index));

  QVariant data(index.data());
  if (data.canConvert<QPixmap>()) {
    QPixmap pixmap(qvariant_cast<QPixmap>(data));
    size.setWidth(qMax(size.width(), pixmap.width() + (int)margin * 2));
    size.setHeight(qMax(size.height(), pixmap.height() + (int)margin * 2));
  }
  return size;
}

bool ButtonDelegate::editorEvent(
  QEvent *event, QAbstractItemModel *,
  QStyleOptionViewItem const &option, QModelIndex const &index)
{
  if (event->type() == QEvent::MouseButtonRelease) {
    QMouseEvent *mouseEvent = static_cast<QMouseEvent*>(event);
    if (mouseEvent) {
      QVariant data(index.data());
      if (data.canConvert<QPixmap>()) {
        QPixmap pixmap(qvariant_cast<QPixmap>(data));
        if (rect(pixmap, option).contains(mouseEvent->pos()))
          emit clicked(index);
      }
    }
  }
  return false;
}
