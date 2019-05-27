#ifndef TAILTABLE_H_190515
#define TAILTABLE_H_190515
/* Exactly like a QTableView but with a status line offering to create a chart with
 * selected columns */
#include <QWidget>
#include <QTableView>
#include <QList>

class TailModel;
class TailTableBar;

class TailTable : public QWidget
{
  Q_OBJECT

  QTableView *tableView;
  TailTableBar *tableBar;
  QList<int> selectedColumns;

public:
  TailTable(TailModel *, QWidget *parent = nullptr);
  QAbstractItemModel *model() const { return tableView->model(); }

private slots:
  void enableBar(QItemSelection const &, QItemSelection const &);
  void extendSelection(QModelIndex const &parent, int first, int last);
  void enrichQuickPlotClicked();

signals:
  void quickPlotClicked(QList<int> const &selectedColumns);
};

#endif
