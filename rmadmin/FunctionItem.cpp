#include <QDateTime>
#include "FunctionItem.h"
#include "GraphView.h"

FunctionItem::FunctionItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *settings, unsigned paletteSize) :
  GraphItem(treeParent, name, settings, paletteSize)
{
  // TODO: updateArrows should reallocate the channels:
  channel = std::rand() % settings->numArrowChannels;
  setZValue(3);
}

FunctionItem::~FunctionItem() {}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

std::vector<std::pair<QString const, QString const>> FunctionItem::labels() const
{
  std::vector<std::pair<QString const, QString const>> labels;
  labels.reserve(8);

  if (isUsed && !(*isUsed))
    labels.emplace_back("", "UNUSED");
  if (startupTime) {
    QDateTime dt = QDateTime::fromSecsSinceEpoch(*startupTime);
    labels.emplace_back("startup", dt.toString());
  }
  if (eventTimeMin) {
    QDateTime dt = QDateTime::fromSecsSinceEpoch(*eventTimeMin);
    labels.emplace_back("min e-time", dt.toString());
  }
  if (eventTimeMax) {
    QDateTime dt = QDateTime::fromSecsSinceEpoch(*eventTimeMax);
    labels.emplace_back("max e-time", dt.toString());
  }
  if (totalTuples)
    labels.emplace_back("#tuples", QString::number(*totalTuples));
  if (totalBytes)
    labels.emplace_back("#bytes", QString::number(*totalBytes));
  if (totalCpu)
    labels.emplace_back("tot CPU", QString::number(*totalCpu));
  if (maxRAM)
    labels.emplace_back("max RAM", QString::number(*maxRAM));

  return labels;
}

QRectF FunctionItem::operationRect() const
{
  return
    QRect(0, 0,
          settings->gridWidth - 2 * (
            settings->functionMarginHoriz +
            settings->programMarginHoriz +
            settings->siteMarginHoriz),
          settings->gridHeight - (
            settings->functionMarginBottom + settings->programMarginBottom +
            settings->siteMarginBottom + settings->functionMarginTop +
            settings->programMarginTop + settings->siteMarginTop));
}
