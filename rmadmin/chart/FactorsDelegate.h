#ifndef FACTORSDELEGATE_H_190802
#define FACTORSDELEGATE_H_190802
#include <QStringList>
#include <QStyledItemDelegate>

class FactorsDelegate : public QStyledItemDelegate
{
  Q_OBJECT
  Q_DISABLE_COPY(FactorsDelegate);

  QStringList columns;

public:
  FactorsDelegate(QObject *parent = nullptr);

  void setColumns(QStringList const &);

  void paint(QPainter *, QStyleOptionViewItem const &,
             QModelIndex const &) const override;

  QSize sizeHint(QStyleOptionViewItem const &,
                 QModelIndex const &) const override;

  QWidget *createEditor(
    QWidget *, QStyleOptionViewItem const &,
    QModelIndex const &) const override;

  void setEditorData(QWidget *editor, QModelIndex const &index) const override;

  void setModelData(QWidget *editor, QAbstractItemModel *model,
                    QModelIndex const &index) const override;

  void updateEditorGeometry(
    QWidget *editor, QStyleOptionViewItem const &option,
    QModelIndex const &index) const override;
};

#endif
