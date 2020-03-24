#ifndef COLORDELEGATE_H_200317
#define COLORDELEGATE_H_200317
#include <QStyledItemDelegate>

class ColorDelegate : public QStyledItemDelegate
{
  Q_OBJECT
  Q_DISABLE_COPY(ColorDelegate);

public:
  ColorDelegate(QObject *parent = nullptr);

  void paint(QPainter *painter, QStyleOptionViewItem const &option,
             QModelIndex const &index) const override;

  QSize sizeHint(QStyleOptionViewItem const &option,
                 QModelIndex const &index) const override;

  QWidget *createEditor(
    QWidget *parent, QStyleOptionViewItem const &option,
    QModelIndex const &index) const override;

  void setEditorData(QWidget *editor, QModelIndex const &index) const override;

  void setModelData(QWidget *editor, QAbstractItemModel *model,
                    QModelIndex const &index) const override;
};

#endif
