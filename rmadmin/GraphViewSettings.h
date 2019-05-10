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
  unsigned labelsLineHeight;

  unsigned
    gridWidth, gridHeight,
    functionMarginHoriz, programMarginHoriz, siteMarginHoriz,
    functionMarginTop, programMarginTop, siteMarginTop,
    functionMarginBottom, programMarginBottom, siteMarginBottom;

  GraphViewSettings();
  ~GraphViewSettings();
  QPointF pointOfTile(unsigned x, unsigned y) const;
};

#endif
