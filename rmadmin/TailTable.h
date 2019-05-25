#ifndef TAILTABLE_H_190515
#define TAILTABLE_H_190515
/* Exactly like a QTableView but with a status line offering to create a chart with
 * selected columns */
#include <QWidget>
#include <QTableView>

class TailTableBar;

class TailTable : public QWidget
{
  Q_OBJECT

  QTableView *tableView;
  TailTableBar *tableBar;

public:
  TailTable(QWidget *parent = nullptr);
  void setModel(QAbstractItemModel *model) { tableView->setModel(model); }
  QAbstractItemModel *model() const { return tableView->model(); }
};

#endif
