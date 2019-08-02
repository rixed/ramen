#include <iostream>
#include <QPixmap>
#include <QPainter>
#include <QMouseEvent>
#include "ButtonDelegate.h"

ButtonDelegate::ButtonDelegate(unsigned margin_, QObject *parent) :
  QStyledItemDelegate(parent),
  margin(margin_)
{}

QPoint ButtonDelegate::pos(
  QPixmap const &pixmap, QStyleOptionViewItem const &option) const
{
  // Right-middle aligned. TODO: left, up, down, middle, center, etc...
  return QPoint(option.rect.right() - pixmap.width() - margin,
                option.rect.center().y() - pixmap.height() / 2);
}

void ButtonDelegate::paint(
  QPainter *painter, QStyleOptionViewItem const &option,
  QModelIndex const &index) const
{
  QStyledItemDelegate::paint(painter, option, index);
  if (! (option.state & QStyle::State_MouseOver)) return;

  QVariant data = index.data();
  if (data.canConvert<QPixmap>()) {
    QPixmap pixmap = qvariant_cast<QPixmap>(data);
    painter->drawPixmap(pos(pixmap, option), pixmap);
  }
}

QSize ButtonDelegate::sizeHint(
  QStyleOptionViewItem const &option,
  QModelIndex const &index) const
{
  QSize size(QStyledItemDelegate::sizeHint(option, index));

  QVariant data = index.data();
  if (data.canConvert<QPixmap>()) {
    QPixmap pixmap = qvariant_cast<QPixmap>(data);
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
      QVariant data = index.data();
      if (data.canConvert<QPixmap>()) {
        QPixmap pixmap = qvariant_cast<QPixmap>(data);
        QRect rect = pixmap.rect().translated(pos(pixmap, option));
        if (rect.contains(mouseEvent->pos()))
          emit clicked(index);
      }
    }
  }
  return false;
}
