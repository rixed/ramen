#ifndef STORAGETIMELINE_H_190522
#define STORAGETIMELINE_H_190522
#include <string>
#include <QWidget>
#include "conf.h"

/* Here we want to display two things:
 *
 * - A timeline of all archives, per worker.
 * - A combo to select any worker and if a time range is selected in the
 *   timeline then a button is enabled that ask for a explain analyze and then
 *   the replayers will be highlighted (the line and the used time ranges).
 *
 * This need to be a view connected to the graph model.
 * A TimeLineView, using the same filtering than the StorageTreeView.
 *
 * Technically, The first component is a custom FormLayout fed from the model,
 * when the label is the function name and the widget is the BinaryHeatLine,
 * connected to the GraphModel.
 * Lines are added in/removed from the FormLayout that is kept in name order.
 * Since the FormLayout will own the BinaryHeatLine widgets, the TimeLineGroup
 * will have to also be told that the pointers are gone.
 *
 * On top and at the bottom of the form are two TimeLines (in the FormLayout
 * with no label).
 *
 * At the very bottom a single line form can be found that ask ramen to
 * explain a query, ie tells what archives are needed to fulfill a given
 * replay query.
 */

class FunctionItem;
class FunctionSelector;
class GraphModel;
struct KValue;
class QPushButton;
class TimeLineView;
class TimeRangeEdit;

class StorageTimeline : public QWidget
{
  Q_OBJECT

  TimeLineView *timeLineView;
  FunctionSelector *explainTarget;
  TimeRangeEdit *explainTimeRange;
  QPushButton *explainButton;
  QPushButton *explainReset;
  std::string respKey;

  void receiveExplain(std::string const &, KValue const &);

public:
  StorageTimeline(GraphModel *, QWidget *parent = nullptr);

protected slots:
  void enableExplainButton(FunctionItem *);
  void requestQueryPlan();
  void resetQueryPlan();
  void onChange(QList<ConfChange> const &);
};

#endif
