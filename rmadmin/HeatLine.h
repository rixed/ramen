#ifndef HEATLINE_H_191204
#define HEATLINE_H_191204
/* A HeatLine is an AbstractTimeLine that displays colored blocks. */
#include <optional>
#include <QColor>
#include <QPair>
#include <QVector>
#include "AbstractTimeLine.h"

class HeatLine : public AbstractTimeLine
{
  Q_OBJECT

public:
  // Created empty:
  HeatLine(
    qreal beginOftime, qreal endOfTime,
    bool withCursor = true,
    bool doScroll = true,
    QWidget *parent = nullptr);

  void add(qreal, std::optional<QColor> const &);
  void reset() { blocks.clear(); }

protected:
  void paintEvent(QPaintEvent *event) override;

private:
  QVector<QPair<qreal, std::optional<QColor>>> blocks;
};

#endif
