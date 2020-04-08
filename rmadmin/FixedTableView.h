#ifndef FIXEDTABLEVIEW_H_200408
#define FIXEDTABLEVIEW_H_200408
/* A QTableView with a sizeHint that ask for the actual content size */
#include <QTableView>
#include <QSize>

class FixedTableView : public QTableView
{
  Q_OBJECT

public:
  FixedTableView(QWidget *parent = nullptr);

  QSize sizeHint() const override;
  QSize minimumSizeHint() const override;
};

#endif
