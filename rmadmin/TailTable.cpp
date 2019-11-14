#include <QVBoxLayout>
#include <QTableView>
#include <QScrollBar>
#include <QLabel>
#include "FunctionItem.h"
#include "TailTableBar.h"
#include "Chart.h"
#include "TailModel.h"
#include "TailTable.h"

TailTable::TailTable(std::shared_ptr<TailModel> tailModel_,
                     std::shared_ptr<PastData> pastData_,
                     QWidget *parent) :
  QSplitter(Qt::Vertical, parent),
  tailModel(tailModel_),
  pastData(pastData_)
{
  QVBoxLayout *topLayout = new QVBoxLayout;

  tableView = new QTableView(this);
  tableView->setModel(tailModel.get());
  tableView->setAlternatingRowColors(true);
  topLayout->addWidget(tableView);
  tableView->setSelectionBehavior(QAbstractItemView::SelectColumns);
  tableView->setSelectionMode(QAbstractItemView::MultiSelection);
  QItemSelectionModel *sm = tableView->selectionModel();
  connect(sm, &QItemSelectionModel::selectionChanged,
          this, &TailTable::enableBar);

  tableBar = new TailTableBar(this);
  topLayout->addWidget(tableBar);
  tableBar->setEnabled(false);

  QWidget *top = new QWidget;
  top->setLayout(topLayout);
  addWidget(top);

  chart = new Chart(tailModel, pastData, selectedColumns, this);
  addWidget(chart);

  /* When the model grows we need to manually extend the selection or the
   * last values of a selected column won't be selected, with the effect that
   * the column will no longer be considered selected: */
  connect(tailModel.get(), &TailModel::rowsInserted,
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
  selectedColumns.clear();
  for (int col = 0; col < numColumns; col ++) {
    if (sm->isSelected(model()->index(0, col, QModelIndex()))) {
      selectedColumns.push_back(col);
    }
  }

  tableBar->setEnabled(! selectedColumns.empty());
}

void TailTable::extendSelection(QModelIndex const &parent, int first, int)
{
  QItemSelectionModel *sm = tableView->selectionModel();
  for (int col : selectedColumns) {
    sm->select(model()->index(first, col, parent),
               QItemSelectionModel::Select |
               QItemSelectionModel::Columns);
  }

  /* Also, if the former last line happen to be viewable, make sure the new
   * last line is also visible (aka auto-scroll): */
  QScrollBar *scrollBar = tableView->verticalScrollBar();
  if (scrollBar->value() == scrollBar->maximum())
    tableView->scrollToBottom();
}

void TailTable::showQuickPlot()
{
  if (chart) {
    chart->setParent(nullptr);
    delete chart;
  }
  chart = new Chart(tailModel, pastData, selectedColumns, this);
  chart->updateGraphic();
}
