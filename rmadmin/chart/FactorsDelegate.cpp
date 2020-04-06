#include <QAbstractItemModel>
#include <QColor>
#include <QColorDialog>
#include <QModelIndex>
#include <QPainter>
#include <QStyleOptionViewItem>
#include "chart/FactorsEditor.h"

#include "chart/FactorsDelegate.h"

FactorsDelegate::FactorsDelegate(QObject *parent)
  : QStyledItemDelegate(parent)
{}

void FactorsDelegate::setColumns(QStringList const &columns_)
{
  columns = columns_;
}

void FactorsDelegate::paint(
  QPainter *painter, QStyleOptionViewItem const &option,
  QModelIndex const &index) const
{
  painter->fillRect(option.rect, painter->background());

  QPixmap pixmap(qvariant_cast<QPixmap>(index.data()));
  int const x(
    option.rect.x() + (option.rect.width() - pixmap.width()) /  2);
  int const y(
    option.rect.y() + (option.rect.height() - pixmap.height()) /  2);
  painter->drawPixmap(x, y, pixmap);
}

QSize FactorsDelegate::sizeHint(
  QStyleOptionViewItem const &,
  QModelIndex const &index) const
{
  QPixmap pixmap(qvariant_cast<QPixmap>(index.data()));
  return pixmap.size();
}

QWidget *FactorsDelegate::createEditor(
  QWidget *parent, QStyleOptionViewItem const &,
  QModelIndex const &) const
{
  FactorsEditor *editor(new FactorsEditor(columns, parent));
  editor->setWindowFlags(Qt::Popup);
  return editor;
}

void FactorsDelegate::setEditorData(QWidget *editor_, QModelIndex const &index) const
{
  QStringList factors(index.data(Qt::EditRole).toStringList());
  FactorsEditor *editor(static_cast<FactorsEditor *>(editor_));
  editor->setCurrentFactors(factors);
}

void FactorsDelegate::setModelData(QWidget *editor_, QAbstractItemModel *model,
                  QModelIndex const &index) const
{
  FactorsEditor *editor(static_cast<FactorsEditor *>(editor_));
  model->setData(index, editor->currentFactors());
}

void FactorsDelegate::updateEditorGeometry(
  QWidget *editor, QStyleOptionViewItem const &,
  QModelIndex const &) const
{
  QSize const hint(editor->sizeHint());
  QPoint p(QCursor::pos());
  editor->setGeometry(p.x(), p.y(), hint.width(), hint.height());
}
