#include <cassert>
#include <QAbstractItemModel>
#include <QDebug>
#include <QModelIndex>
#include <QPainter>
#include <QPixmap>
#include <QStyleOptionViewItem>
#include "IconSelector.h"

#include "RollButtonDelegate.h"

RollButtonDelegate::RollButtonDelegate(QObject *parent)
  : QStyledItemDelegate(parent), minSize(QSize(0, 0))
{}

void RollButtonDelegate::addIcon(QPixmap const icon)
{
  minSize.setWidth(std::max(minSize.width(), icon.width()));
  minSize.setHeight(std::max(minSize.height(), icon.height()));

  pixmaps.append(icon);
}

void RollButtonDelegate::paint(
  QPainter *painter, QStyleOptionViewItem const &option,
  QModelIndex const &index) const
{
  int const n(index.data().toInt());
  assert(n < pixmaps.count());

  IconSelector::paint(*painter, option.rect, pixmaps, n);
}

QSize RollButtonDelegate::sizeHint(
  QStyleOptionViewItem const &, QModelIndex const &) const
{
  return minSize;
}

QWidget *RollButtonDelegate::createEditor(
  QWidget *parent, QStyleOptionViewItem const &,
  QModelIndex const &) const
{
  IconSelector *editor = new IconSelector(pixmaps, parent);
  connect(editor, &IconSelector::selectionChanged,
          this, &RollButtonDelegate::commitAndCloseEditor);
  return editor;
}

void RollButtonDelegate::setEditorData(
  QWidget *editor_, QModelIndex const &index) const
{
  int const n(index.data().toInt());
  assert(n < pixmaps.count());

  IconSelector *editor(static_cast<IconSelector *>(editor_));
  editor->setSelected(n);
}

void RollButtonDelegate::setModelData(
  QWidget *editor_, QAbstractItemModel *model, QModelIndex const &index) const
{
  IconSelector *editor(static_cast<IconSelector *>(editor_));
  model->setData(index, editor->selected());
}

void RollButtonDelegate::commitAndCloseEditor()
{
  IconSelector *editor(static_cast<IconSelector *>(sender()));
  emit commitData(editor);
  emit closeEditor(editor);
}
