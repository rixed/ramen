#include <QMarginsF>
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "Resources.h"
#include "GraphModel.h"
#include "GraphView.h"

ProgramItem::ProgramItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *settings) :
  GraphItem(treeParent, name, settings)
{
  setZValue(2);
}

ProgramItem::~ProgramItem()
{
  for (FunctionItem *function : functions) {
    delete function;
  }
}

void ProgramItem::reorder(GraphModel const *model)
{
  for (int i = 0; (size_t)i < functions.size(); i++) {
    if (functions[i]->row != i) {
      functions[i]->row = i;
      functions[i]->setPos(30, i * 30);
      emit model->positionChanged(model->createIndex(i, 0, static_cast<GraphItem *>(functions[i])));
    }
  }
  prepareGeometryChange();
}

std::vector<std::pair<QString const, QString const>> ProgramItem::labels() const
{
  return {{ "#functions", QString::number(functions.size()) }};
}

QRectF ProgramItem::operationRect() const
{
  QRectF bbox;
  for (auto function : functions) {
    QRectF b = function->operationRect();
    b.translate(function->pos());
    bbox |= b;
  }
  bbox += QMarginsF(settings->functionMarginHoriz,
                    settings->functionMarginTop,
                    settings->functionMarginHoriz,
                    settings->functionMarginBottom);
  return bbox;
}

// All their running functions are top-halves
bool ProgramItem::isTopHalf() const
{
  if (0 == functions.size()) return false;

  for (auto &function : functions) {
    if (! function->isTopHalf()) return false;
  }
  return true;
}

QVariant ProgramItem::data(int column, int role) const
{
  if (role == Qt::DisplayRole && !isTopHalf()) {
    if (column == GraphModel::EditButton)
      return Resources::get()->infoPixmap;
    else if (column == GraphModel::TableButton)
      return Resources::get()->tablePixmap;
  }

  return GraphItem::data(column, role);
}
