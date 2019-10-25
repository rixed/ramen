#include "LoggerView.h"

LoggerView::LoggerView(QWidget *parent) :
  QListView(parent)
{
  model = new QStringListModel();
  setModel(model);
}

void LoggerView::addLog(const QString &log)
{
  model->insertRows(0, 1);
  QModelIndex new_log_index = model->index(0);
  model->setData(new_log_index , log);
}
