#include <QDebug>
#include <QSplitter>
#include <QVBoxLayout>
#include <QWidget>
#include "chart/TimeChart.h"
#include "chart/TimeChartEditForm.h"

#include "chart/TimeChartEditor.h"

TimeChartEditor::TimeChartEditor(
  std::string const &key, QWidget *parent)
  : QWidget(parent)
{
  editPanel = new TimeChartEditForm(key);
  chart = new TimeChart();

  QSplitter *splitter = new QSplitter;
  splitter->addWidget(editPanel);
  splitter->addWidget(chart);

  // layout
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(splitter);
  setLayout(layout);
}
