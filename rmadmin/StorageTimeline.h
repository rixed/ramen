#ifndef STORAGETIMELINE_H_190522
#define STORAGETIMELINE_H_190522
#include <QWidget>
/* Here we want to display two things:
 * - A timeline of all archives, per worker.
 * - A combo to select any worker and if a time range is selected in the
 *   timeline then a button is enabled that ask for a explain analyze and then
 *   the replayers will be highlighted (the line and the used time ranges).
 *
 * This need to be a view connected to the graph model.
 * A TimeLineView, using the same filtering than the StorageTreeView?
 *
 * Technically, The first component is a custom FormLayout fed from the model,
 * when the label is the function name and the widget is the BinaryHeatLine,
 * connected to the model (or rather to the StorageTreeModel).
 * Lines are added/removed in/from the FormLayout that is kept in name order.
 * Since the FormLayout will own the BinaryHeatLine widgets, the TimeLineGroup
 * will have to also be told that the pointers are gone. The old assumption
 * that it won't add new lines after some have been deleted does not hold
 * any more. So the view will have to tell him.
 *
 * On top and at the bottom of the form are two TimeLines (in the FormLayout
 * with no label).
 *
 * The second component is trivial enough.
 *
 * For highlighting, ... dunno.
 */

class GraphModel;
class TimeLineView;

class StorageTimeline : public QWidget
{
  Q_OBJECT

  TimeLineView *timeLineView;

public:
  StorageTimeline(GraphModel *, QWidget *parent = nullptr);
};

#endif
