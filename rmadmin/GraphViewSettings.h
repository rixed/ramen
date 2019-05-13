#ifndef GRAPHVIEWSETTINGS_H_190510
#define GRAPHVIEWSETTINGS_H_190510
#include <QFont>
#include <QPointF>

class GraphViewSettings
{
public:
  QFont labelsFont;
  int labelsLineHeight;
  QFont titleFont;
  int titleLineHeight;

  int
    gridWidth, gridHeight,
    functionMarginHoriz, programMarginHoriz, siteMarginHoriz,
    functionMarginTop, programMarginTop, siteMarginTop,
    functionMarginBottom, programMarginBottom, siteMarginBottom;
  int labelsHorizMargin;

  unsigned numArrowChannels;
  int arrowChannelWidth;
  int arrowWidth;
  int arrowConnectInY;
  int arrowConnectOutY;

  GraphViewSettings();
  ~GraphViewSettings();
  QPointF pointOfTile(int x, int y) const;
};

#endif
