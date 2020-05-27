#include <QDebug>
#include <QFormLayout>
#include <QLabel>
#include "BinaryHeatLine.h"
#include "confValue.h"
#include "FunctionItem.h"
#include "GraphModel.h"
#include "misc.h"
#include "TimeLineGroup.h"
#include "TimeLine.h"
#include "TimeRange.h"
#include "TimeLineView.h"

static bool const verbose { false };

TimeLineView::TimeLineView(
    GraphModel *graphModel_,
    QWidget *parent)
  : QWidget(parent),
    graphModel(graphModel_)
{
  formLayout = new QFormLayout;
  formLayout->setSpacing(0);
  formLayout->setLabelAlignment(Qt::AlignLeft);
  // counter weird MacOS default:
  formLayout->setFieldGrowthPolicy(QFormLayout::AllNonFixedFieldsGrow);
  timeLineGroup = new TimeLineGroup(this);

  /* Default, will be overridden as soon as we receive actual archives to
   * display: */
  qreal const endOfTime = getTime();
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
    if (verbose) qDebug() << "TimeLineView: Added function has no shr!?";
    return;
  }
  /* Same if the function has no archive: */
  std::shared_ptr<conf::TimeRange const> archivedTimes = shr->archivedTimes;
  if (! archivedTimes || archivedTimes->isEmpty()) {
    if (verbose)
      qDebug() << "TimeLineView: Added function" << shr->name
               << "has no archives";
    return;
  }

  QString name(functionItem->fqName());
  /* Insert a line in the form: */
  int i;
  for (i = 0; i < labels.count(); i ++) {
    int const c = name.compare(labels[i]);
    if (c == 0) {
      if (verbose)
        qDebug() << "TimeLineView: Added function already present:" << name;
      BinaryHeatLine *heatLine = static_cast<BinaryHeatLine *>(
        formLayout->itemAt(i + 1, QFormLayout::FieldRole)->widget());
      heatLine->setArchivedTimes(*archivedTimes);
      return;
    } else if (c < 0) {
      if (verbose)
        qDebug() << "TimeLineView: Have to insert" << name
                 << "before" << labels[i];
      break;
    }
  }

  BinaryHeatLine *heatLine = new BinaryHeatLine(
    archivedTimes->range[0].t1,
    archivedTimes->range[archivedTimes->range.size()-1].t2);

  heatLine->setArchivedTimes(*archivedTimes);

  if (verbose)
    qDebug() << "TimeLineView: Adding a heatline for" << name
             << "starting at" << stringOfDate(archivedTimes->range[0].t1)
             << "at row" << i << "/" << formLayout->rowCount();
  timeLineGroup->add(heatLine);
  formLayout->insertRow(i + 1, name, heatLine);
  labels.insert(i, name);
}

void TimeLineView::removeTimeLine(FunctionItem const *functionItem)
{
  QString name(functionItem->fqName());

  for (int i = 0; i < labels.count(); i ++) {
    if (labels[i] != name) continue;

    if (verbose)
      qDebug() << "TimeLineView: Removing function" << name;
    BinaryHeatLine *heatLine = static_cast<BinaryHeatLine *>(
      formLayout->itemAt(i + 1, QFormLayout::FieldRole)->widget());
    // Owned by the form so remove from the group first:
    timeLineGroup->remove(heatLine);
    formLayout->removeRow(i + 1);
    labels.removeAt(i);
    return;
  }
}

void TimeLineView::highlightRange(QString const &label, QPair<qreal, qreal> const range)
{
  for (int i = 0; i < labels.count(); i ++) {
    if (labels[i] != label) continue;

    if (verbose)
      qDebug() << "TimeLineView: Highlighting function" << label;
    BinaryHeatLine *heatLine = static_cast<BinaryHeatLine *>(
      formLayout->itemAt(i + 1, QFormLayout::FieldRole)->widget());
    heatLine->highlightRange(range);

    return;
  }

  qWarning() << "TimeLineView: Unknown label" << label;
}

void TimeLineView::resetHighlights()
{
  for (int i = 0; i < labels.count(); i ++) {
    BinaryHeatLine *heatLine = static_cast<BinaryHeatLine *>(
      formLayout->itemAt(i + 1, QFormLayout::FieldRole)->widget());
    heatLine->resetHighlights();
  }
}
