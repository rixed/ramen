#ifndef GRAPHVIEWSETTINGS_H_190510
#define GRAPHVIEWSETTINGS_H_190510
#include <QFont>
#include <QFontMetrics>
#include <QPointF>

class GraphViewSettings
{
public:
  QFont labelsFont;
  QFontMetrics labelsFontMetrics;
  int labelsLineHeight;

  int
    gridWidth, gridHeight,
    functionMarginHoriz, programMarginHoriz, siteMarginHoriz,
    functionMarginTop, programMarginTop, siteMarginTop,
    functionMarginBottom, programMarginBottom, siteMarginBottom;

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
