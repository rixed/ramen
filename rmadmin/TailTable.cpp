#include <iostream>
#include <QVBoxLayout>
#include <QTableView>
#include "TailTableBar.h"
#include "TailModel.h"
#include "TailTable.h"

TailTable::TailTable(TailModel *model, QWidget *parent) :
  QWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;

  tableView = new QTableView(this);
  tableView->setModel(model);
  layout->addWidget(tableView);
  tableView->setSelectionBehavior(QAbstractItemView::SelectColumns);
  tableView->setSelectionMode(QAbstractItemView::MultiSelection);
  QItemSelectionModel *sm = tableView->selectionModel();
  connect(sm, &QItemSelectionModel::selectionChanged, this, &TailTable::enableBar);

  tableBar = new TailTableBar(this);
  layout->addWidget(tableBar);
  tableBar->setEnabled(false);

  setLayout(layout);

  /* When the model grows we need to manually extend the selection or the
   * last values of a selected column won't be selected, with the effect that
   * the column will no longer be considered selected: */
  connect(model, &TailModel::rowsInserted, this, &TailTable::extendSelection);
}

void TailTable::enableBar(QItemSelection const &, QItemSelection const &)
{
  QItemSelectionModel *sm = tableView->selectionModel();
  // Save the column numbers:
  int const numColumns = model()->columnCount();
  unsigned numSelected = 0;
  selectedColumns.clear();
  for (int col = 0; col < numColumns; col++) {
    if (sm->isSelected(model()->index(0, col, QModelIndex()))) {
      selectedColumns.append(col);
      numSelected ++;
    }
  }

  tableBar->setEnabled(numSelected > 0);
}

void TailTable::extendSelection(QModelIndex const &parent, int first, int)
{
  QItemSelectionModel *sm = tableView->selectionModel();
  for (auto &col : selectedColumns) {
    sm->select(model()->index(first, col, parent),
               QItemSelectionModel::Select | QItemSelectionModel::Columns);
  }
}
