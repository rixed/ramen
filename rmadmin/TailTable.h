#ifndef TAILTABLE_H_190515
#define TAILTABLE_H_190515
/* Widget that displays the tail of some function in a table, with selectable
 * columns, then a control bar offering to show and tailor the corresponding
 * chart, and also do add it to some dashboard. */
#include <memory>
#include <QSplitter>
#include <QTableView>
#include <QList>
#include <QStackedLayout>

class TailModel;
class TailTableBar;
class Chart;

class TailTable : public QSplitter
{
  Q_OBJECT

  QTableView *tableView;
  TailTableBar *tableBar;
  QList<int> selectedColumns;
  std::shared_ptr<TailModel> tailModel;
  Chart *chart; // or null

  QAbstractItemModel *model() const { return tableView->model(); }

public:
  TailTable(std::shared_ptr<TailModel>, QWidget *parent = nullptr);

private slots:
  void enableBar(QItemSelection const &, QItemSelection const &);
  void extendSelection(QModelIndex const &parent, int first, int last);
  void showQuickPlot();

  // TODO: signal willingness to add a chart to some dashboard
};

#endif
