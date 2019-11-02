#include <QtGlobal>
#include <QDebug>
#include <QTableWidget>
#include <QHeaderView>
#include "confValue.h"
#include "TimeRangeViewer.h"

TimeRangeViewer::TimeRangeViewer(QWidget *parent) :
  AtomicWidget(parent)
{
  table = new QTableWidget(1, 2);
  table->setHorizontalHeaderLabels({ tr("Since"), tr("Until") });
  table->setMinimumWidth(400);
  table->setCornerButtonEnabled(false);
  table->setEditTriggers(QAbstractItemView::NoEditTriggers);
  table->verticalHeader()->setVisible(false);
  relayoutWidget(table);
}

bool TimeRangeViewer::setValue(
  std::string const &, std::shared_ptr<conf::Value const> v)
{
  /* Empty the previous table */
  table->setRowCount(0); // Keep the header

  std::shared_ptr<conf::TimeRange const> timeRange =
    std::dynamic_pointer_cast<conf::TimeRange const>(v);
  if (timeRange) {
    size_t sz = timeRange->range.size();
    table->setRowCount(sz);
    for (unsigned i = 0; i < sz; i ++) {
      conf::TimeRange::Range const &r = timeRange->range[i];
      QTableWidgetItem *since =
        new QTableWidgetItem(stringOfDate(r.t1));
      QTableWidgetItem *until =
        new QTableWidgetItem(stringOfDate(r.t2) + (
          r.openEnded ? QString("…") : QString()));
      table->setItem(i, 0, since);
      table->setItem(i, 1, until);
    }
    table->resizeColumnsToContents();
    return true;
  } else {
    qCritical() << "Not a TimeRange value?!";
    return false;
  }
}
