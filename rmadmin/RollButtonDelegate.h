#ifndef ROLLBUTTONDELEGATE_H_200315
#define ROLLBUTTONDELEGATE_H_200315
/* An item delegate that displays one amongst several possible icons. */
#include <QList>
#include <QSize>
#include <QStyledItemDelegate>

class QPixmap;

class RollButtonDelegate : public QStyledItemDelegate
{
  Q_OBJECT
  Q_DISABLE_COPY(RollButtonDelegate);

  QList<QPixmap> pixmaps;

  QSize minSize;

public:
  RollButtonDelegate(QObject *parent = nullptr);

  void addIcon(QPixmap);

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

  void updateEditorGeometry(
    QWidget *editor, QStyleOptionViewItem const &option,
    QModelIndex const &index) const override;

private slots:
  void commitAndCloseEditor();
};

#endif
