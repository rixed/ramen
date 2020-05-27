#ifndef NOTIFTIMELINE_H_191204
#define NOTIFTIMELINE_H_191204
#include <optional>
#include <string>
#include "chart/AbstractTimeLine.h"

class NotifTimeLine : public AbstractTimeLine
{
  Q_OBJECT

  std::string const incidentId;

public:
  // Created empty:
  NotifTimeLine(
    std::string const incidentId,
    qreal beginOfTime, qreal endOfTime,
    bool withCursor = true,
    bool doScroll = true,
    QWidget *parent = nullptr);

protected:
  void paintEvent(QPaintEvent *event) override;
};

#endif
