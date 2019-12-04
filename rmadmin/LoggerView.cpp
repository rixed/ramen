#include <iostream>
#include "LoggerView.h"

LoggerView::LoggerView(QWidget *parent) :
  QListView(parent)
{
  model = new QStringListModel;
  setModel(model);
}

void LoggerView::addLog(const QString &log)
{
  if (model->insertRows(0, 1)) {
    QModelIndex firstLine(model->index(0, 0));
    if (firstLine.isValid())
      model->setData(firstLine, log, Qt::DisplayRole);
  } else {
    std::cerr << log.toStdString() << std::endl;
  }
}
