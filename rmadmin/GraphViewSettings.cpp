#include <QFontMetrics>
#include "GraphViewSettings.h"

GraphViewSettings::GraphViewSettings()
{
  labelsFont.setPixelSize(9);
  QFontMetrics fm(labelsFont);
  labelsLineHeight = fm.lineSpacing();

  titleFont.setPixelSize(13);
  titleFont.setBold(true);
  fm = QFontMetrics(titleFont);
  titleLineHeight = fm.lineSpacing();

  gridWidth = 340;
  gridHeight = 300;

  numArrowChannels = 5;
  arrowChannelWidth = 10;
  arrowWidth = 6;

  siteMarginHoriz = 50;
  programMarginHoriz = 5;
  functionMarginHoriz = 5;

  siteMarginTop = 50;
  programMarginTop = 1.5 * titleLineHeight;
  functionMarginTop = 1.5 * titleLineHeight;

  siteMarginBottom = 50;
  programMarginBottom = 5;
  functionMarginBottom = 5;

  labelsHorizMargin = 10;

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


