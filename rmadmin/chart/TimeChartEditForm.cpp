#include <QLabel>
#include <QVBoxLayout>
#include "chart/TimeChartEditWidget.h"

#include "chart/TimeChartEditForm.h"

TimeChartEditForm::TimeChartEditForm(
  std::string const &key,
  QWidget *parent)
  : AtomicForm(parent)
{
  editWidget = new TimeChartEditWidget(this);
  editWidget->setKey(key);
  addWidget(editWidget);

  setCentralWidget(editWidget);
}
