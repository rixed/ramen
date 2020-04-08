#include <QHeaderView>
#include <QMargins>

#include "FixedTableView.h"

FixedTableView::FixedTableView(QWidget *parent)
  : QTableView(parent)
{
  setVerticalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
  setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
}

QSize FixedTableView::sizeHint() const
{
  QHeaderView const *vHeader = verticalHeader();
  QHeaderView const *hHeader = horizontalHeader();
  int const frame { frameWidth() };
  QMargins const m { contentsMargins() };
  return QSize(
    frame + m.left() + hHeader->length() +
    vHeader->sizeHint().width() + m.right() + frame,
    frame + m.top() + hHeader->sizeHint().height() +
    vHeader->length() + m.bottom() + frame);
}

QSize FixedTableView::minimumSizeHint() const
{
  return sizeHint();
}
