#ifndef BINARYHEATLINE_H_191204
#define BINARYHEATLINE_H_191204
/* A BinaryHeatLine is a HeatLine with only on or off values and a simplified
 * interface. */
#include <optional>
#include <QColor>
#include <QPair>
#include <QVector>
#include "HeatLine.h"

namespace conf {
  struct TimeRange;
};

class BinaryHeatLine : public HeatLine
{
  Q_OBJECT

public:
  BinaryHeatLine(
    qreal beginOftime, qreal endOfTime,
    bool withCursor = true,
    bool doScroll = true,  // As there is almost no content in there
    QWidget *parent = nullptr);

  void add(qreal start, qreal stop);
  void setArchivedTimes(conf::TimeRange const &);
};

#endif
