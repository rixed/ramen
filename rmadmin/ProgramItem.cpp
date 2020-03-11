#include <QMarginsF>
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "Resources.h"
#include "GraphModel.h"
#include "GraphView.h"

ProgramItem::ProgramItem(
  GraphItem *treeParent, std::unique_ptr<Program> program,
  GraphViewSettings const *settings) :
  GraphItem(treeParent, std::move(program), settings)
{
  setZValue(2);
}

void ProgramItem::reorder(GraphModel const *model)
{
  for (int i = 0; (size_t)i < functions.size(); i++) {
    if (functions[i]->row != i) {
      functions[i]->row = i;
      functions[i]->setPos(30, i * 30);
      emit model->positionChanged(model->createIndex(
        i, 0, static_cast<GraphItem *>(functions[i])));
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
  for (auto const &function : functions) {
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

bool ProgramItem::isUsed() const
{
  if (0 == functions.size()) return false;

  for (auto &function : functions) {
    if (function->isUsed()) return true;
  }
  return false;
}

bool ProgramItem::isRunning() const
{
  if (0 == functions.size()) return false;

  for (auto &function : functions) {
    if (function->isRunning()) return true;
  }
  return false;
}

bool ProgramItem::isWorking() const
{
  if (0 == functions.size()) return false;

  for (auto &function : functions) {
    if (function->isWorking()) return true;
  }
  return false;
}

QVariant ProgramItem::data(int column, int role) const
{
  if (role == Qt::DisplayRole && !isTopHalf()) {
    if (column == GraphModel::ActionButton2)
      return Resources::get()->infoPixmap;
  }

  return GraphItem::data(column, role);
}

ProgramItem::operator QString() const
{
  QString s(" Program[");
  s += row;
  s += "]:";
  s += shared->name;
  for (FunctionItem const *function : functions) {
    s += *function;
  }
  return s;
}
