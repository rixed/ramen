#include <QLabel>
#include <QScrollBar>
#include <QTableView>
#include <QVBoxLayout>
#include "FunctionItem.h"
#include "TailModel.h"

#include "TailTable.h"

TailTable::TailTable(std::shared_ptr<TailModel> tailModel_,
                     QWidget *parent)
  : QWidget(parent),
    tailModel(tailModel_)
{
  tableView = new QTableView(this);
  tableView->setModel(tailModel.get());
  tableView->setAlternatingRowColors(true);
  tableView->setSelectionBehavior(QAbstractItemView::SelectColumns);
  tableView->setSelectionMode(QAbstractItemView::MultiSelection);

  /* When the model grows we need to manually extend the selection or the
   * last values of a selected column won't be selected, with the effect that
   * the column will no longer be considered selected: */
  connect(tailModel.get(), &TailModel::rowsInserted,
          this, &TailTable::extendSelection);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(tableView);
  setLayout(layout);
}

void TailTable::extendSelection(QModelIndex const &parent, int first, int)
{
  QItemSelectionModel *sm = tableView->selectionModel();
  int const numColumns = model()->columnCount();
  for (int col = 0; col < numColumns; col ++) {
    if (sm->isSelected(model()->index(0, col, QModelIndex()))) {
      sm->select(model()->index(first, col, parent),
                 QItemSelectionModel::Select |
                 QItemSelectionModel::Columns);
    }
  }

  /* Also, if the former last line happen to be viewable, make sure the new
   * last line is also visible (aka auto-scroll): */
  QScrollBar *scrollBar = tableView->verticalScrollBar();
  if (scrollBar->value() == scrollBar->maximum())
    tableView->scrollToBottom();
}
