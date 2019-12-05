#ifndef TIMELINE_H_191204
#define TIMELINE_H_191204
/* A Timeline is an AbstractTimeLine that displays an axis with tick
 * marks and labels.
 */
#include "AbstractTimeLine.h"

class TimeLine : public AbstractTimeLine
{
  Q_OBJECT

public:
  enum TicksPosition {
    TicksTop    = 0x1,
    TicksBottom = 0x2,
  };

  TimeLine(
    qreal beginOftime, qreal endOfTime,
    TicksPosition = TicksTop,
    bool withCursor = true,
    QWidget *parent = nullptr);

  QSize sizeHint() const override { return QSize(250, 40); }

protected:
  void paintEvent(QPaintEvent *event) override;

private:
  TicksPosition ticksPosition;
};

#endif
