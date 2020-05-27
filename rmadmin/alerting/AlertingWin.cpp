#include <QWidget>
#include <QVBoxLayout>
#include "alerting/AlertingLogsModel.h"
#include "alerting/AlertingJournal.h"
#include "alerting/AlertingStats.h"
#include "alerting/AlertingTimeLine.h"

#include "alerting/AlertingWin.h"

AlertingWin::AlertingWin(QWidget *parent)
  : SavedWindow("Alerting", tr("Alerting"), true, parent)
{
  QWidget *widget = new QWidget;

  QVBoxLayout *layout = new QVBoxLayout;

  stats = new AlertingStats;
  layout->addWidget(stats);

  timeLine = new AlertingTimeLine;
  layout->addWidget(timeLine);

  journal = new AlertingJournal(AlertingLogsModel::globalLogsModel);
  layout->addWidget(journal);

  widget->setLayout(layout);

  setCentralWidget(widget);
}
