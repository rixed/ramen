#include "GraphViewSettings.h"

GraphViewSettings::GraphViewSettings() :
  labelsFontMetrics(labelsFont)
{
  labelsLineHeight =
    labelsFontMetrics.height() + ((labelsFontMetrics.height() + 4) / 5);

  gridWidth = 250, gridHeight = 150,
  functionMarginHoriz = labelsLineHeight,
  programMarginHoriz = labelsLineHeight,
  siteMarginHoriz = labelsLineHeight,
  functionMarginTop = 20, programMarginTop = 20, siteMarginTop = 30,
  functionMarginBottom = 10, programMarginBottom = 10, siteMarginBottom = 10;
}

GraphViewSettings::~GraphViewSettings() {};

QPointF GraphViewSettings::pointOfTile(unsigned x, unsigned y) const
{
  return QPointF(x * gridWidth, y * gridHeight);
}


