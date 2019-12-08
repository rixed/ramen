#include <QDebug>
#include <QFormLayout>
#include <QLabel>
#include "BinaryHeatLine.h"
#include "confValue.h"
#include "FunctionItem.h"
#include "GraphModel.h"
#include "TimeLineGroup.h"
#include "TimeLine.h"
#include "TimeRange.h"
#include "TimeLineView.h"

TimeLineView::TimeLineView(
    GraphModel *graphModel_,
    QWidget *parent)
  : QWidget(parent),
    graphModel(graphModel_)
{
  formLayout = new QFormLayout;
  formLayout->setSpacing(0);
  timeLineGroup = new TimeLineGroup(this);

  /* Default, will be overridden as soon as we receive actual archives to
   * display: */
  qreal const endOfTime = TimeRange::now();
  qreal const beginOfTime = endOfTime - 24*3600;
  TimeLine *timeLineTop =
    new TimeLine(beginOfTime, endOfTime, TimeLine::TicksBottom);
  TimeLine *timeLineBottom =
    new TimeLine(beginOfTime, endOfTime, TimeLine::TicksTop);
  /* Note: Order of insertion in the group has no influence over order of
   * representation in the QFormLayout: */
  timeLineGroup->add(timeLineTop);
  timeLineGroup->add(timeLineBottom);

  /* *Last* line will always be this timeLine. Lines will be added/removed
   * above to maintain a list of known archiving functions: */
  formLayout->addRow(QString(), timeLineTop);
  formLayout->addRow(QString(), timeLineBottom);

  setLayout(formLayout);

  /* Connect with the model: */
  connect(graphModel, &GraphModel::functionAdded,
          this, &TimeLineView::updateOrCreateTimeLine);
  connect(graphModel, &GraphModel::storagePropertyChanged,
          this, &TimeLineView::updateOrCreateTimeLine);
  connect(graphModel, &GraphModel::functionRemoved,
          this, &TimeLineView::removeTimeLine);
}

void TimeLineView::updateOrCreateTimeLine(FunctionItem const *functionItem)
{
  /* Some functions may be added before their shr is created or populated.
   * deal with them in the dataChanged slot. */
  std::shared_ptr<Function const> shr =
    std::static_pointer_cast<Function const>(functionItem->shared);
  if (! shr) {
    qDebug() << "TimeLineView: added function has no shr";
    return;
  }
  /* Same if the function has no archive: */
  std::shared_ptr<conf::TimeRange const> archivedTimes = shr->archivedTimes;
  if (! archivedTimes || archivedTimes->isEmpty()) {
    qDebug() << "TimeLineView: added function has no archives";
    return;
  }

  QString name(functionItem->fqName());
  /* Insert a line in the form: */
  int i;
  for (i = 1; i < formLayout->rowCount() - 1; i ++) {
    QLabel *label = static_cast<QLabel *>(
      formLayout->itemAt(i, QFormLayout::LabelRole)->widget());
    int const c = label->text().compare(name);
    if (c == 0) {
      qDebug() << "TimeLineView: Added function already present:" << name;
      BinaryHeatLine *heatLine = static_cast<BinaryHeatLine *>(
        formLayout->itemAt(i, QFormLayout::FieldRole)->widget());
      heatLine->setArchivedTimes(*archivedTimes);
      return;
    } else if (c < 0) break;
  }

  BinaryHeatLine *heatLine = new BinaryHeatLine(
    archivedTimes->range[0].t1,
    archivedTimes->range[archivedTimes->range.size()-1].t2);

  heatLine->setArchivedTimes(*archivedTimes);

  qDebug() << "TimeLineView: adding a heatline for" << name
           << "starting at" << stringOfDate(archivedTimes->range[0].t1);
  timeLineGroup->add(heatLine);
  formLayout->insertRow(i, name, heatLine);
}

void TimeLineView::removeTimeLine(FunctionItem const *functionItem)
{
  QString name(functionItem->fqName());

  for (int i = 1; i < formLayout->rowCount() - 1; i ++) {
    QLabel *label = static_cast<QLabel *>(
      formLayout->itemAt(i, QFormLayout::LabelRole)->widget());
    if (label->text() != name) continue;

    qDebug() << "TimeLineView: Removing function" << name;
    BinaryHeatLine *heatLine = static_cast<BinaryHeatLine *>(
      formLayout->itemAt(i, QFormLayout::FieldRole)->widget());
    // Owned by the form so remove from the group first:
    timeLineGroup->remove(heatLine);
    formLayout->removeRow(i);
    return;
  }
}
