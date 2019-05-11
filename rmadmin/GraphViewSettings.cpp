#include "GraphViewSettings.h"

GraphViewSettings::GraphViewSettings() :
  labelsFontMetrics(labelsFont)
{
  labelsLineHeight = labelsFontMetrics.lineSpacing();

  gridWidth = 300;
  gridHeight = 300;

  numArrowChannels = 5;
  arrowChannelWidth = 10;
  arrowWidth = 6;

  siteMarginHoriz = 75;
  programMarginHoriz = 5;
  functionMarginHoriz = 5;

  siteMarginTop = 75;
  programMarginTop = 1.5 * labelsLineHeight;
  functionMarginTop = 1.5 * labelsLineHeight;

  siteMarginBottom = 75;
  programMarginBottom = 5;
  functionMarginBottom = 5;

  int const functionY0 =
    siteMarginTop + programMarginTop + functionMarginTop;
  int const functionY1 =
    gridHeight -
    (siteMarginBottom + programMarginBottom + functionMarginBottom);
  int const functionYMid = functionY0 + (functionY1 - functionY0) / 2;

  arrowConnectInY = functionYMid - 10;
  arrowConnectOutY = functionYMid + 10;
}

GraphViewSettings::~GraphViewSettings() {};

QPointF GraphViewSettings::pointOfTile(int x, int y) const
{
  return QPointF(x * gridWidth, y * gridHeight);
}


