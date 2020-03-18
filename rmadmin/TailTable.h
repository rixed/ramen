#ifndef TAILTABLE_H_190515
#define TAILTABLE_H_190515
/* Widget that displays the tail of some function in a table, with selectable
 * columns, then a control bar offering to show and tailor the corresponding
 * chart, and also do add it to some dashboard. */
#include <memory>
#include <QTableView>

class TailModel;

class TailTable : public QWidget
{
  Q_OBJECT

  QTableView *tableView;
  std::shared_ptr<TailModel> tailModel;

  QAbstractItemModel *model() const { return tableView->model(); }

public:
  TailTable(std::shared_ptr<TailModel>,
            QWidget *parent = nullptr);

private slots:
  void extendSelection(QModelIndex const &parent, int first, int last);
};

#endif
