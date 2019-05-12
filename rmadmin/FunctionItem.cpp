#include "FunctionItem.h"
#include "GraphView.h"

FunctionItem::FunctionItem(OperationsItem *treeParent, QString const &name, GraphViewSettings const *settings, unsigned paletteSize) :
  OperationsItem(treeParent, name, settings, paletteSize)
{
  // TODO: updateArrows should reallocate the channels:
  channel = std::rand() % settings->numArrowChannels;
  updateFrame();
}

FunctionItem::~FunctionItem() {}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

QRectF FunctionItem::boundingRect() const
{
  qreal b = border();
  return
    QRect(0, 0,
          settings->gridWidth - 2 * (
            settings->functionMarginHoriz +
            settings->programMarginHoriz +
            settings->siteMarginHoriz),
          settings->gridHeight - (
            settings->functionMarginBottom + settings->programMarginBottom +
            settings->siteMarginBottom + settings->functionMarginTop +
            settings->programMarginTop + settings->siteMarginTop))
  + QMargins(b, b, b, b);
}
