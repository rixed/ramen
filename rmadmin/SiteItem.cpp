#include <QMarginsF>
#include "ProgramItem.h"
#include "SiteItem.h"
#include "GraphView.h"
#include "OperationsModel.h"

SiteItem::SiteItem(OperationsItem *treeParent, QString const &name, GraphViewSettings const *settings, unsigned paletteSize) :
  OperationsItem(treeParent, name, settings, paletteSize)
{
  setZValue(1);
}

SiteItem::~SiteItem()
{
  for (ProgramItem *program : programs) {
    delete program;
  }
}

QVariant SiteItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

void SiteItem::reorder(OperationsModel const *model)
{
  for (int i = 0; (size_t)i < programs.size(); i++) {
    if (programs[i]->row != i) {
      programs[i]->row = i;
      programs[i]->setPos(30, i * 90);
      emit model->positionChanged(model->createIndex(i, 0, static_cast<OperationsItem *>(programs[i])));
    }
  }
  prepareGeometryChange();
}

std::vector<std::pair<QString const, QString const>> SiteItem::labels() const
{
  return {
    { "#programs", QString::number(programs.size()) },
    { "master", isMaster ? (*isMaster ? tr("yes") : tr("no")) : tr("unknown") }
  };
}

QRectF SiteItem::operationRect() const
{
  QRectF bbox;
  for (auto program : programs) {
    QRectF b = program->operationRect();
    b.translate(program->pos());
    bbox |= b;
  }
  bbox += QMarginsF(settings->programMarginHoriz,
                    settings->programMarginTop,
                    settings->programMarginHoriz,
                    settings->programMarginBottom);
  return bbox;
}

