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

class Chart;
class PastData;
class TailModel;
class TailTableBar;

class TailTable : public QSplitter
{
  Q_OBJECT

  QTableView *tableView;
  TailTableBar *tableBar;
  std::vector<int> selectedColumns;
  std::shared_ptr<TailModel> tailModel;
  std::shared_ptr<PastData> pastData;
  Chart *chart; // or null

  QAbstractItemModel *model() const { return tableView->model(); }

public:
  TailTable(std::shared_ptr<TailModel>,
            std::shared_ptr<PastData>,
            QWidget *parent = nullptr);

private slots:
  void enableBar(QItemSelection const &, QItemSelection const &);
  void extendSelection(QModelIndex const &parent, int first, int last);
  void showQuickPlot();

  // TODO: signal willingness to add a chart to some dashboard
};

#endif
