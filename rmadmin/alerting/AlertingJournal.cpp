#include <QHeaderView>
#include <QTableView>
#include <QVBoxLayout>
#include "AlertingLogsModel.h"

#include "AlertingJournal.h"

AlertingJournal::AlertingJournal(AlertingLogsModel *model, QWidget *parent)
  : QWidget(parent)
{
  tableView = new QTableView;
  tableView->setSelectionMode(QAbstractItemView::NoSelection);
  tableView->setShowGrid(false);
  tableView->horizontalHeader()->setStretchLastSection(true);
  tableView->horizontalHeader()->setSectionResizeMode(QHeaderView::ResizeToContents);
  tableView->verticalHeader()->hide();
  tableView->setEditTriggers(QAbstractItemView::NoEditTriggers);
  tableView->setModel(model);

  QVBoxLayout *layout = new QVBoxLayout;
  layout->setContentsMargins(QMargins());
  layout->addWidget(tableView);

  setLayout(layout);

  connect(model, &AlertingLogsModel::rowsInserted,
          this, &AlertingJournal::resizeColumns);
}

void AlertingJournal::resizeColumns()
{
  int const rows { tableView->model()->rowCount() };
  if (rows < 16 || (rows & 15) == 0)
    tableView->resizeColumnToContents(0);
}
