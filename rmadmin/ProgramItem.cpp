#include <QMarginsF>
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "OperationsModel.h"
#include "GraphView.h"

ProgramItem::ProgramItem(OperationsItem *treeParent, QString const &name, GraphViewSettings const *settings, unsigned paletteSize) :
  OperationsItem(treeParent, name, settings, paletteSize)
{
  updateFrame();
}

ProgramItem::~ProgramItem()
{
  for (FunctionItem *function : functions) {
    delete function;
  }
}

QVariant ProgramItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

void ProgramItem::reorder(OperationsModel const *model)
{
  for (int i = 0; (size_t)i < functions.size(); i++) {
    if (functions[i]->row != i) {
      functions[i]->row = i;
      functions[i]->setPos(30, i * 30);
      emit model->positionChanged(model->createIndex(i, 0, static_cast<OperationsItem *>(functions[i])));
    }
  }
  updateFrame();
}

void ProgramItem::addLabels(std::vector<std::pair<QString const, QString const>> *labels) const
{
  labels->emplace_back("#functions", QString::number(functions.size()));
}

QRectF ProgramItem::boundingRect() const
{
  QRectF bbox;
  for (auto function : functions) {
    QRectF b = function->boundingRect();
    b.translate(function->pos());
    bbox |= b;
  }
/*  std::cout << "prog bbox1 = " << bbox.x() << ", " << bbox.y()
            << " + " << bbox.width() << ", " << bbox.height() << '\n';*/
  bbox += QMarginsF(settings->functionMarginHoriz,
                    settings->functionMarginTop,
                    settings->functionMarginHoriz,
                    settings->functionMarginBottom);
/*  std::cout << "prog bbox2 = " << bbox.x() << ", " << bbox.y()
            << " + " << bbox.width() << ", " << bbox.height() << '\n';*/

  qreal b = border();
  bbox += QMargins(b, b, b, b);
  return bbox;
}
