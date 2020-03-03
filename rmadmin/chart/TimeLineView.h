#ifndef TIMELINEVIEW_H_191206
#define TIMELINEVIEW_H_191206
#include <QList>
#include <QWidget>

/* A widget displaying a group of named timelines, with a TimeLine ruler on top
 * and at the bottom.
 *
 * Not inheriting from an QAbstractItemView because this is specific to
 * the GraphModel. Could be turned into a more generic TimeLine viewer later
 * with some work. */

class FunctionItem;
class GraphModel;
class QFormLayout;
class TimeLineGroup;

class TimeLineView : public QWidget
{
  Q_OBJECT

  GraphModel *graphModel; // Needed for direct access to the GraphItems
  QFormLayout *formLayout;
  /* Copy of the labels in plain text form
   * (because of: https://github.com/rixed/ramen/issues/1094) */
  QList<QString> labels;

  TimeLineGroup *timeLineGroup;

public:
  TimeLineView(GraphModel *, QWidget *parent = nullptr);

  void highlightRange(QString const &label, QPair<qreal, qreal> const range);
  void resetHighlights();

protected slots:
  void updateOrCreateTimeLine(FunctionItem const *);
  void removeTimeLine(FunctionItem const *);
};

#endif
