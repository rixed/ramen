#include "FunctionItem.h"
#include "GraphView.h"

FunctionItem::FunctionItem(OperationsItem *treeParent, QString const &name, GraphViewSettings const *settings) :
  OperationsItem(treeParent, name, settings, Qt::blue)
{
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
  return QRect(0, 0,
               settings->gridWidth - 2 * (
                 settings->functionMarginHoriz +
                 settings->programMarginHoriz +
                 settings->siteMarginHoriz),
               settings->gridHeight - (
                 settings->functionMarginBottom + settings->programMarginBottom +
                 settings->siteMarginBottom + settings->functionMarginTop +
                 settings->programMarginTop + settings->siteMarginTop));
}
