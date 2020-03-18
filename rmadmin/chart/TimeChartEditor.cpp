#include <QDebug>
#include <QPushButton>
#include <QSplitter>
#include <QVBoxLayout>
#include <QWidget>
#include "chart/TimeChart.h"
#include "chart/TimeChartEditForm.h"
#include "chart/TimeLine.h"
#include "chart/TimeLineGroup.h"

#include "chart/TimeChartEditor.h"

TimeChartEditor::TimeChartEditor(
  std::string const &key, QWidget *parent)
  : QWidget(parent)
{
  editForm = new TimeChartEditForm(key);
  editForm->setVisible(false);

  chart = new TimeChart(editForm->editWidget);
  connect(chart, &TimeChart::newTailTime,
          this, &TimeChartEditor::newTailTime);

  TimeLine *timeLine = new TimeLine(0., 600., TimeLine::TicksTop);
  timeLine->setMinimumHeight(30);
  timeLine->setMaximumHeight(50);

  timeLineGroup = new TimeLineGroup(this);
  timeLineGroup->add(chart);
  timeLineGroup->add(timeLine);
  connect(this, &TimeChartEditor::timeRangeChanged,
          timeLineGroup, &TimeLineGroup::setTimeRange);

  QVBoxLayout *timeLinesLayout = new QVBoxLayout;
  timeLinesLayout->addWidget(chart);
  timeLinesLayout->addWidget(timeLine);
  timeLinesLayout->setSpacing(0);
  timeLines = new ResizedWidget;
  timeLines->setLayout(timeLinesLayout);

  // out of flow button to open/hide the editor:
  openEditor = new QPushButton(tr("Edit"), timeLines);
  openEditor->raise();
  connect(timeLines, &ResizedWidget::resized,
          [this]() {
    openEditor->move(QPoint(0, timeLines->height() - openEditor->height()));
  });
  connect(openEditor, &QPushButton::clicked,
          [this]() {
    editForm->setVisible(true);
    editForm->wantEdit();
    openEditor->setVisible(false);
  });
  connect(editForm, &TimeChartEditForm::changeEnabled,
          [this](bool enabled) {
    if (enabled) return;
    editForm->setVisible(false);
    openEditor->setVisible(true);
  });

  QSplitter *splitter = new QSplitter;
  splitter->addWidget(editForm);
  splitter->addWidget(timeLines);

  // layout
  QVBoxLayout *layout = new QVBoxLayout;
  layout->addWidget(splitter);
  setLayout(layout);
}

void ResizedWidget::resizeEvent(QResizeEvent *event)
{
  QWidget::resizeEvent(event);
  emit resized();
}
