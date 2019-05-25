#include <QVBoxLayout>
#include <QTableView>
#include "TailTableBar.h"
#include "TailTable.h"

TailTable::TailTable(QWidget *parent) :
  QWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;

  tableView = new QTableView(this);
  layout->addWidget(tableView);

  tableBar = new TailTableBar(this);
  layout->addWidget(tableBar);

  setLayout(layout);
}
