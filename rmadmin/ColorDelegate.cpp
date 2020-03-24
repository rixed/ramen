#include <QAbstractItemModel>
#include <QColor>
#include <QColorDialog>
#include <QModelIndex>
#include <QPainter>
#include <QStyleOptionViewItem>

#include "ColorDelegate.h"

ColorDelegate::ColorDelegate(QObject *parent)
  : QStyledItemDelegate(parent)
{}

void ColorDelegate::paint(
  QPainter *painter, QStyleOptionViewItem const &option,
  QModelIndex const &index) const
{
  QColor c(index.data().value<QColor>());
  painter->fillRect(option.rect, c);
}

QSize ColorDelegate::sizeHint(
  QStyleOptionViewItem const &,
  QModelIndex const &) const
{
  return QSize(10, 10); // ?
}

QWidget *ColorDelegate::createEditor(
  QWidget *parent, QStyleOptionViewItem const &,
  QModelIndex const &) const
{
  QColorDialog *editor(new QColorDialog(parent));
  editor->setOption(QColorDialog::ShowAlphaChannel);
  return editor;
}

void ColorDelegate::setEditorData(QWidget *editor_, QModelIndex const &index) const
{
  QColor c(index.data().value<QColor>());
  QColorDialog *editor(static_cast<QColorDialog *>(editor_));
  editor->setCurrentColor(c);
}

void ColorDelegate::setModelData(QWidget *editor_, QAbstractItemModel *model,
                  QModelIndex const &index) const
{
  QColorDialog *editor(static_cast<QColorDialog *>(editor_));
  model->setData(index, editor->currentColor());
}
