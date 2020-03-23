#ifndef TIMELINEGROUP_H_191205
#define TIMELINEGROUP_H_191205
/* This object is used to group together a set of AbstractTimeLine objects.
 * The grouping is logical not graphical: it merely connects all the widgets
 * together so that they share a single cursor, time range and viewport.
 * But it displays nothing (and is not a QWidget).
 * Consequently, it does not take ownership of the objects. */
#include <QObject>
#include <QVector>
#include <QPair>

class AbstractTimeLine;
struct TimeRange;

class TimeLineGroup : public QObject
{
  Q_OBJECT

  QVector<AbstractTimeLine *> items;
  qreal minBeginOfTime, maxEndOfTime;
  QPair<qreal, qreal> largestViewPort;

public:
  TimeLineGroup(QObject *parent = nullptr);

  /* Does not take ownership of the passed timeline. */
  void add(AbstractTimeLine *);
  /* Won't complain if that timeline is not in the group: */
  void remove(QObject *);

protected slots:
  void setCurrentTimes(qreal) const;
  void setBeginOfTimes(qreal) const;
  void setEndOfTimes(qreal) const;

public slots:
  void setTimeRange(TimeRange const &) const;
};

#endif
