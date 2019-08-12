#include <iostream>
#include <QVBoxLayout>
#include <QTableView>
#include "FunctionItem.h"
#include "TailTableBar.h"
#include "Chart.h"
#include "ChartDataSet.h"
#include "TailModel.h"
#include "TailTable.h"

TailTable::TailTable(std::shared_ptr<Function> function_, QWidget *parent) :
  QWidget(parent),
  function(function_),
  chart(nullptr)
{
  layout = new QVBoxLayout;

  if (! function->tailModel)
    function->tailModel = new TailModel(function);
  function->tailModel->setUsed(true);

  tableView = new QTableView(this);
  tableView->setModel(function->tailModel);
  tableView->setAlternatingRowColors(true);
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
  connect(function->tailModel, &TailModel::rowsInserted,
          this, &TailTable::extendSelection);

  /* Enrich the quickPlotClicked signal with the selected columns: */
  connect(tableBar, &TailTableBar::quickPlotClicked,
          this, &TailTable::showQuickPlot);
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

void TailTable::showQuickPlot()
{
  if (chart) delete chart;

  chart = new Chart(this);
  layout->addWidget(chart);

  std::shared_ptr<RamenType const> outType = function->outType();
  /* Make a chartDataSet out of each column: */
  for (auto col : selectedColumns) {
    ChartDataSet *ds = new ChartDataSet(function, col);
    chart->addData(ds);
  }
  chart->update();
}
