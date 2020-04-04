#include <string>
#include <QCompleter>
#include <QDebug>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QScrollArea>
#include <QVBoxLayout>
#include "conf.h"
#include "GraphModel.h"
#include "FunctionItem.h"
#include "FunctionSelector.h"
#include "ReplayRequest.h"
#include "Resources.h"
#include "chart/TimeLineView.h"
#include "TimeRange.h"
#include "TimeRangeEdit.h"
#include "StorageTimeline.h"

static bool const verbose(false);

StorageTimeline::StorageTimeline(
  GraphModel *graphModel,
  QWidget *parent)
  : QWidget(parent)
{
  QVBoxLayout *layout = new QVBoxLayout;

  /*
   * The TimeLineView in a QScrollArea:
   */

  QScrollArea *scrollArea = new QScrollArea;
  scrollArea->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
  scrollArea->setVerticalScrollBarPolicy(Qt::ScrollBarAlwaysOn);
  scrollArea->setWidgetResizable(true);

  // Required for the scrollarea to behave properly, for some reason:
  QWidget *widget = new QWidget;
  QVBoxLayout *l1 = new QVBoxLayout;
  widget->setLayout(l1);

  timeLineView = new TimeLineView(graphModel);
  l1->addWidget(timeLineView);
  l1->setSizeConstraint(QLayout::SetMinimumSize);

  scrollArea->setWidget(widget);  // Must be added last for some reason

  layout->addWidget(scrollArea);

  /*
   * Explain: a "combo" to select the target, a time picker and a button
   */

  explainTarget = new FunctionSelector(graphModel);
  explainTarget->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Minimum);
  explainTimeRange = new TimeRangeEdit;
  explainButton = new QPushButton(tr("&Explain"));
  // Disable that button until a function is selected:
  explainButton->setEnabled(false);
  explainReset = new QPushButton;
  QIcon closeIcon(resources->get()->closePixmap);
  explainReset->setIcon(closeIcon);
  explainReset->setEnabled(false);

  QHBoxLayout *l2 = new QHBoxLayout;
  l2->addWidget(explainTarget);
  l2->addWidget(explainTimeRange);
  l2->addWidget(explainButton);
  l2->addWidget(explainReset);

  layout->addLayout(l2);

  setLayout(layout);

  /*
   * Connections
   */

  /* Changing the selected function enable/disable the explainButton: */
  connect(explainTarget, &FunctionSelector::selectionChanged,
          this, &StorageTimeline::enableExplainButton);

  /* explainButton triggers the query: */
  connect(explainButton, &QPushButton::clicked,
          this, &StorageTimeline::requestQueryPlan);

  /* explainReset removes the highlights: */
  connect(explainReset, &QPushButton::clicked,
          this, &StorageTimeline::resetQueryPlan);

  /* Get the query answer: */
  connect(kvs, &KVStore::keyChanged,
          this, &StorageTimeline::onChange);
}

void StorageTimeline::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
      case KeyChanged:
        receiveExplain(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

void StorageTimeline::enableExplainButton(FunctionItem *functionItem)
{
  explainButton->setEnabled(functionItem != nullptr);
}

void StorageTimeline::requestQueryPlan()
{
  if (respKey.empty()) {
    if (! my_socket) {
      qCritical() << "Cannot request a query plan before socket is known!";
      return;
    }

    respKey =
      "clients/" + *my_socket + "/response/" + respKeyPrefix + "explain";
  }

  FunctionItem *functionItem = explainTarget->getCurrent();
  if (! functionItem) return;

  std::shared_ptr<Function> function =
    std::static_pointer_cast<Function>(functionItem->shared);

  std::string functionName(function->name.toStdString());
  std::string programName(function->programName.toStdString());
  std::string siteName(function->siteName.toStdString());

  double since, until;
  explainTimeRange->range.absRange(&since, &until);

  std::shared_ptr<conf::ReplayRequest const> req =
    std::make_shared<conf::ReplayRequest const>(
      siteName, programName, functionName,
      since, until, true, respKey);

  if (verbose)
    qDebug() << "StorageTimeline: request QueryPlan for target"
             << functionItem->fqName()
             << "and response key" << QString::fromStdString(respKey);

  askSet("replay_requests", req);
}

void StorageTimeline::resetQueryPlan()
{
  timeLineView->resetHighlights();
  explainReset->setEnabled(false);
}

void StorageTimeline::receiveExplain(std::string const &key, KValue const &kv)
{
  if (key != respKey) return;

  if (verbose)
    qDebug() << "StorageTimeline: received a query plan:" << *kv.val;

  std::shared_ptr<conf::Replay> replay =
    std::dynamic_pointer_cast<conf::Replay>(kv.val);
  if (! replay) {
    qCritical() << "StorageTimeline: Cannot convert explain answer into "
                   "a Replay!?";
    return;
  }

  timeLineView->resetHighlights();

  QPair<qreal, qreal> range(replay->since, replay->until);

  for (conf::SiteFq const &source : replay->sources) {
    QString const label(source.site + "/" + source.fq);
    timeLineView->highlightRange(label, range);
  }

  explainReset->setEnabled(true);
}
